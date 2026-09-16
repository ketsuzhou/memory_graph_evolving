package ports

// RoomLifecycleStore is the Host-owned persistence seam for benchmark Room
// lifecycle coordination. Existing adapters satisfy it structurally.
type RoomLifecycleStore interface {
	RoomStore
	DeliveryStore
}
