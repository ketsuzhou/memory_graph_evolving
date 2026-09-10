// Package embedding contains standard-library adapters for embedding provider
// seams. It does not log requests, responses, input content, or credentials.
package embedding

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"

	"river2.dev/graph-memory-service/internal/ports"
)

const (
	PolicyOpenAICompatibleV1  = "openai-compatible-v1"
	maxEmbeddingResponseBytes = 16 << 20
)

type OpenAICompatible struct {
	client  *http.Client
	baseURL string
	apiKey  string
	model   string
	key     string
}

func NewOpenAICompatible(client *http.Client, baseURL, apiKey, model, policy string) (*OpenAICompatible, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	model = strings.TrimSpace(model)
	if client == nil {
		return nil, fmt.Errorf("embedding provider: HTTP client is required")
	}
	if baseURL == "" || model == "" {
		return nil, fmt.Errorf("embedding provider: base URL and model are required")
	}
	if policy != PolicyOpenAICompatibleV1 {
		return nil, fmt.Errorf("embedding provider: unsupported request policy %q", policy)
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return nil, fmt.Errorf("embedding provider: base URL must be an absolute HTTP or HTTPS URL")
	}
	if apiKey != "" && parsedURL.Scheme != "https" {
		return nil, fmt.Errorf("embedding provider: API keys require an HTTPS base URL")
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	digest := sha256.Sum256([]byte(policy + "\x00" + baseURL + "\x00" + model))
	return &OpenAICompatible{
		client: &clientCopy, baseURL: baseURL, apiKey: apiKey, model: model,
		key: "openai-compatible:" + hex.EncodeToString(digest[:]),
	}, nil
}

func (provider *OpenAICompatible) CacheKey() string { return provider.key }

func (provider *OpenAICompatible) Embed(ctx context.Context, inputs []string) ([][]float64, error) {
	if len(inputs) == 0 {
		return [][]float64{}, nil
	}
	body, err := json.Marshal(struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}{Model: provider.model, Input: inputs})
	if err != nil {
		return nil, fmt.Errorf("embedding provider: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embedding provider: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if provider.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+provider.apiKey)
	}
	response, err := provider.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding provider: request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding provider: unexpected HTTP status %d", response.StatusCode)
	}
	var payload struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	limitedBody, err := io.ReadAll(io.LimitReader(response.Body, maxEmbeddingResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("embedding provider: read response: %w", err)
	}
	if len(limitedBody) > maxEmbeddingResponseBytes {
		return nil, fmt.Errorf("embedding provider: response exceeds %d bytes", maxEmbeddingResponseBytes)
	}
	if err := json.Unmarshal(limitedBody, &payload); err != nil {
		return nil, fmt.Errorf("embedding provider: decode response: %w", err)
	}
	if len(payload.Data) != len(inputs) {
		return nil, fmt.Errorf("embedding provider: returned %d vectors for %d inputs", len(payload.Data), len(inputs))
	}
	vectors := make([][]float64, len(inputs))
	dimension := 0
	for _, item := range payload.Data {
		if item.Index < 0 || item.Index >= len(inputs) || vectors[item.Index] != nil {
			return nil, fmt.Errorf("embedding provider: invalid or duplicate result index")
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("embedding provider: returned an empty vector")
		}
		if dimension == 0 {
			dimension = len(item.Embedding)
		} else if len(item.Embedding) != dimension {
			return nil, fmt.Errorf("embedding provider: returned inconsistent vector dimensions")
		}
		for _, value := range item.Embedding {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("embedding provider: returned a non-finite vector value")
			}
		}
		vectors[item.Index] = append([]float64(nil), item.Embedding...)
	}
	for _, vector := range vectors {
		if vector == nil {
			return nil, fmt.Errorf("embedding provider: response omitted a result index")
		}
	}
	return vectors, nil
}

var _ ports.EmbeddingProvider = (*OpenAICompatible)(nil)
