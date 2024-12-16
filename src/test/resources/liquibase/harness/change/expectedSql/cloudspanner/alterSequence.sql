CREATE SEQUENCE test_sequence OPTIONS (sequence_kind='bit_reversed_positive', start_with_counter = 10)
ALTER SEQUENCE test_sequence SET OPTIONS (skip_range_min = 1000, skip_range_max = 5000000)