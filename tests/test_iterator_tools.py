from nominal._utils import iterator_tools


def test_batched_less_than_batch_size():
    batches = list(iterator_tools.batched(range(5), n=10))
    assert len(batches) == 1
    assert batches[0] == tuple(range(5))


def test_batched_more_than_batch_size():
    batches = list(iterator_tools.batched(range(15), n=10))
    assert len(batches) == 2
    assert batches[0] == tuple(range(10))
    assert batches[1] == tuple(range(10, 15))


def test_batched_equal_to_batch_size():
    batches = list(iterator_tools.batched(range(10), n=10))
    assert len(batches) == 1
    assert batches[0] == tuple(range(10))


def test_batched_multiple_of_batch_size():
    batches = list(iterator_tools.batched(range(20), n=10))
    assert len(batches) == 2
    assert batches[0] == tuple(range(10))
    assert batches[1] == tuple(range(10, 20))
