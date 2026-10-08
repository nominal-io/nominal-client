import random


def generate_sparkline(length=10):
    """Generate a random sparkline in ASCII."""
    data = [random.randint(1, 10) for _ in range(length)]
    chars = "▁▂▃▄▅▆▇█"
    max_value = max(data)
    min_value = min(data)

    def normalize(value):
        if max_value == min_value:
            return 0
        return int((value - min_value) / (max_value - min_value) * (len(chars) - 1))

    sparkline = "".join(chars[normalize(value)] for value in data)
    return sparkline
