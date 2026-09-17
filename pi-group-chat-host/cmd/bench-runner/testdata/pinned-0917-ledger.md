CONSOLIDATED SKILL
name: use bit count bound for power-of-two partition
trigger: When you need to determine if an integer T can be written as a sum of exactly k powers of 2 (possibly repeated), especially in problems involving iterative subtraction of (power_of_2 + constant) terms
steps: 1. Compute T = target value you need to partition. 2. Check T >= k (minimum sum of k powers of 2 is k × 2^0). 3. Check T.bit_count() <= k (each set bit represents one power; you can always split larger powers into smaller ones to increase the count). 4. Both conditions together are necessary and sufficient.
pitfalls: Without the bit_count check, you may accept impossible targets. Without the T >= k check, you may accept targets too small to require k terms. Missing the insight that splitting powers works (2^n = 2^{n-1} + 2^{n-1}) leads to overly complex solutions.

CONSOLIDATED SKILL
name: decrement_decimal_string_in_place
trigger: Need to compute n-1 where n is given as a decimal string with 50+ digits, especially when n might be a power of 10
steps: 1. Convert string to list of characters for mutability. 2. Scan from right to left finding the first non-'0' digit. 3. Decrement that digit and change all trailing '0's to '9'. 4. If the first digit becomes '0' and length > 1, remove the leading zero.
pitfalls: Using int(s)-1 then str() for huge strings (100+ digits) incurs O(n²) conversion cost and can timeout; also fails silently if intermediate int overflows in languages without arbitrary precision.

CONSOLIDATED SKILL
name: verify_expectations_before_modifying_code
trigger: when a test case appears to fail and you're about to modify your algorithm, or when finalizing a competitive programming solution with sample inputs
steps: 1. Manually trace through the input step-by-step using the exact formula/logic from the problem statement. 2. Write down each intermediate calculation explicitly (indices chosen, values used, arithmetic operations). 3. Execute the code against all provided sample inputs and verify outputs match exactly. 4. Only after confirming your expected value is correct, investigate whether the code produced it.
pitfalls: Modifying working code based on incorrect manual calculations wastes time and introduces new bugs. Submitting untested code that passes mentally but fails on edge cases or off-by-one errors.

CONSOLIDATED SKILL
name: detect_brute_force_timing_risk
trigger: Task allows modifying one element and evaluating a result; the evaluation itself takes linear or near-linear time; input size constraint is >= 10^3
steps: 1. Calculate total operations as (number of candidate modifications) × (cost per evaluation). 2. If this product exceeds ~10^7-10^8 operations, the brute force will time out even if logically correct. 3. Before submitting, look for patterns where the evaluation can be computed incrementally or cached across similar modifications.
pitfalls: Submitting an algorithmically correct but timing-unaware solution that passes small examples but fails hidden large inputs due to O(n²) or worse complexity.

CONSOLIDATED SKILL
name: check_distinct_values_instead_of_gcd
trigger: Array reduction problems where operations can generate smaller values from existing elements
steps: 1. Check if all array elements are identical using len(set(nums)) == 1. 2. If all identical, answer is ceil(count/2) since you can only pair equal values. 3. If at least two distinct values exist, answer is 1 because you can generate smaller remainders through modulo operations.
pitfalls: Computing GCD is unnecessary complexity that introduces import dependencies and obscures the real invariant.

CONSOLIDATED SKILL
name: verify_required_imports_before_using_functions
trigger: Writing Python solutions that use standard library functions like gcd, reduce, etc.
steps: 1. Before using any function, check if it's built-in or requires import. 2. Add necessary imports at the top (e.g., from math import gcd, from functools import reduce). 3. Test that imports resolve before submitting.
pitfalls: Using functions without importing them causes NameError at runtime, breaking otherwise correct logic.

CONSOLIDATED SKILL
name: stop_at_first_mismatch_for_sequential_pair_consumption
trigger: Problem requires repeatedly consuming the first two elements of an array/sequence, where all operations must share the same computed value (e.g., sum), and you must find the maximum number of valid consecutive operations
steps: 1. Compute the target value from the first pair (indices 0 and 1). 2. Iterate through consecutive non-overlapping pairs starting from index 0, incrementing by 2 each iteration. 3. Count each pair matching the target. 4. Break immediately when a pair mismatches or fewer than 2 elements remain.
pitfalls: Continuing past a mismatch to search for matching pairs elsewhere in the array, or attempting to skip non-matching pairs — the problem requires strictly sequential consumption from the start.

CONSOLIDATED SKILL
name: avoid_greedy_assumption_in_dp_string_segmentation
trigger: DP problem where you segment a string using valid substrings and minimize/maximize count; you compute a maximum reachable length at each position
steps: 1. When computing DP transitions, do not assume taking the maximum valid length is always optimal. 2. Consider all valid substring lengths from the current position, not just the longest. 3. If the valid lengths form a contiguous range [1, max_len], update dp for all positions in that range: for length in 1..max_len: dp[i+length] = min(dp[i+length], dp[i]+1). 4. Only skip this if you can prove greedy works.
pitfalls: Assuming "longer is better" without proof; a shorter prefix at position i might enable reaching a position that leads to fewer total segments.

CONSOLIDATED SKILL
name: verify_heap_based_top_k_over_complex_tree_structures
trigger: when selecting top-k values from a dynamically growing collection during iteration, especially with value ranges up to 10^6
steps: 1. Before implementing segment trees or Fenwick trees for top-k queries, check if a simple max-heap suffices (push/pop O(log k) vs tree construction O(max_value)). 2. If using a heap, maintain exactly k elements and track their sum; when size exceeds k, pop smallest. 3. Reserve segment trees only when you need range queries on indices, not just value selection.
pitfalls: Building O(max_value) data structures wastes memory and time; segment tree implementations have subtle bugs that are hard to debug under pressure.

CONSOLIDATED SKILL
name: handle_duplicates_in_sorted_iteration
trigger: when iterating through sorted data where ties affect query validity (e.g., strict inequality comparisons like "find j where arr[j] < arr[i]") and input may contain duplicates
steps: 1. After sorting, group consecutive elements with identical sort keys before processing. 2. Query the data structure FIRST for all elements in the group (they should see the same state). 3. Update the data structure AFTER all queries for the group complete. 4. Create a minimal test case where all comparison keys are identical and verify the output matches expected behavior. 5. Never interleave queries and updates within a group of equal keys.
pitfalls: Processing one element at a time causes earlier elements in an equal-value group to incorrectly see later elements as valid candidates, violating the strict inequality requirement. Without explicit duplicate testing, algorithms that work on distinct values silently fail on duplicates.

CONSOLIDATED SKILL
name: deduplicate_elements_preserving_min_index
trigger: when marking multiples or divisors using an array that may contain duplicate values, where only the smallest index matters for ties
steps: 1. Before the sieve loop, build a dict mapping each unique value to its first occurrence index. 2. Iterate only over unique values for marking operations. 3. Use the stored original indices when assigning results.
pitfalls: Without deduplication, each duplicate value repeats the full marking loop over all multiples. If elements has k copies of the same value, you waste (k-1) * (max_val / value) operations for no benefit.

CONSOLIDATED SKILL
name: binary_search_monotonic_geometric_measure
trigger: Finding a coordinate value where a cumulative geometric measure (area, length, count) equals a target, when the measure is monotonically increasing with the coordinate; computing the portion of a shape's measure that lies on one side of a cutting line
steps: 1. Identify the search bounds by finding min/max of relevant coordinates across all shapes. 2. Calculate the target value (e.g., half of total area). 3. Binary search on the coordinate, computing the cumulative measure at each midpoint. 4. For each shape, determine its extent relative to the cutting boundary. 5. Handle three cases: completely below (full contribution), completely above (no contribution), or intersected (calculate overlapping portion as a fraction of the shape's dimension). 6. Sum contributions from all shapes independently (don't account for overlaps between shapes unless specified). 7. Continue iterations until precision requirement is met (typically 60-100 iterations for 10^-5 precision).
pitfalls: Skipping the three-case analysis for each shape leads to incorrect area calculations at boundaries. Insufficient iterations fails precision requirements. Missing the "completely outside" case causes errors when the cut is far from a shape. Incorrectly merging overlapping shapes when the problem counts them separately.

CONSOLIDATED SKILL
name: reject_brute_force_for_large_numeric_ranges
trigger: problem asks to iterate/count over a numeric range [l, r] where constraints show r up to 10^9 or larger, especially when about to select an algorithm whose complexity depends directly on input value magnitude
steps: 1. Recognize that O(r-l) iteration will timeout when r can be 10^9. 2. Check if the algorithm iterates over the numeric value itself (not just its digits or bits). 3. If iterating over [1, n] or similar, confirm n is bounded by ~10^7 or less for acceptable runtime. 4. Identify if the problem involves digit-based conditions (sum, product, patterns) and switch to digit dynamic programming or mathematical counting instead of enumeration. 5. When n reaches 10^8+, immediately consider logarithmic/DP approaches. 6. Don't rationalize away large constraints as "acceptable in practice" without evidence.
pitfalls: Submitting O(n) brute force when n can be 10^9 causes TLE. Misreading 10^9 as "okay for brute force" leads to fundamental algorithmic failure. Post-hoc justification like "we'd only check up to the given range" doesn't prevent TLE on adversarial test cases.

CONSOLIDATED SKILL
name: verify_code_and_output_before_submission
trigger: After implementing the core algorithm of a competitive programming problem, before marking TASK_COMPLETE; generating multi-line code solutions where output may be truncated or interrupted
steps: 1. Write the output loop immediately after the algorithm, not as an afterthought. 2. Scan your generated code for obvious truncation signs: incomplete words at line endings, missing closing brackets/parentheses, abrupt statement breaks. 3. Verify all control flow blocks (if/else, loops, functions) have matching opening/closing delimiters. 4. Verify output matches specification exactly (format, special values like "Unreachable"). 5. Ensure the final lines contain complete statements and the program has a clear termination point. 6. Confirm all edge cases in output are handled (unreachable nodes, single-station cases).
pitfalls: Submitting incomplete code despite correct algorithm wastes all prior effort. Submitting truncated code that crashes on execution (NameError, SyntaxError) or produces wrong answers.

CONSOLIDATED SKILL
name: handle_unreachable_in_latest_time_problems
trigger: Computing latest/latest-departure times where some states may have no valid path to goal
steps: 1. Initialize unreachable states with sentinel value (-1 or -infinity) distinct from valid times. 2. Only update state if new time strictly improves AND is valid (>= minimum threshold). 3. Output "Unreachable" (or equivalent) when sentinel persists, never print the sentinel itself.
pitfalls: Printing sentinel values confuses judges. Treating infinity incorrectly causes overflow. Forgetting unreachable case fails tests.

CONSOLIDATED SKILL
name: decompose_impartial_game_by_connected_components
trigger: impartial game where moves remove pairs of items; items can be modeled as nodes in a graph where edges represent valid pairings; need to determine winner with optimal play
steps: 1. Build the pairing graph (nodes=cards, edge between i,j if they can be removed together). 2. Find connected components of this graph. 3. Compute Grundy number (mex of reachable Grundy values) for each component independently. 4. XOR all component Grundy numbers: result≠0 means first player wins, =0 means second player wins.
pitfalls: Attempting full 2^N state exploration when components are independent causes TLE. Computing Grundy numbers naively without component decomposition wastes exponential effort on separable subproblems.

