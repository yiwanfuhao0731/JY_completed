
def get_hash(a, b):

    """

    :rtype: int

    A pseudo 'hash' based on Cantor pairing fucntion

    """

    return 0.5 * (a + b) * (a + b + 1) + b