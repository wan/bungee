from pprint import PrettyPrinter
import re


PRINTER = PrettyPrinter(indent=2)
PP = lambda text: PRINTER.pprint(text)


def prettify(thing):
    """
    Pretty print an object.
    """
    return PRINTER.pformat(thing)


def make_identifier(text):
    """
    Return text as a valid variable identifier.
    """
    text = re.sub('[\. ]', '_', text)
    text = re.sub('[^a-zA-Z0-9_]', '', text)
    text = re.sub('^[^a-zA-Z_]+', '', text)
    if not len(text):
        raise ValueError, "Cannot make identifier from '%s'" % text
    return text

