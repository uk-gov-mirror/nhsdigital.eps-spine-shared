import unicodedata

import six


def handle_encoding_oddities(text, attempt_escaped_replacement=False):
    """
    Strip accents and non-ascii characters from unicode strings
    """
    if not isinstance(text, (six.text_type, six.binary_type)):
        text = six.text_type(text)

    # By default use decomposed characters and simply ignore the combining characters
    form = "NFKD"
    mode = "ignore"

    # Attempt to convert bytes to text
    if isinstance(text, six.binary_type):
        try:
            # We expect UTF-8 normally
            text = text.decode("utf8")
        except UnicodeDecodeError:
            # If that didn't work, use latin1 which basically always works
            text = text.decode("latin1")

            # if replacement is not requested, use composed characters
            # and replace them with question marks when encoding to ascii.
            # This is only done if using the fallback latin1 encoding as a last resort
            if not attempt_escaped_replacement:
                form = "NFKC"
                mode = "replace"

    return unicodedata.normalize(form, text).encode("ascii", mode).decode("ascii")


def quoted(value):
    """
    Utility function that returns the value as a string surrounded by double quotes
    """
    try:
        return '"' + str(value) + '"'
    except (UnicodeEncodeError, UnicodeDecodeError):
        return '"' + handle_encoding_oddities(value) + '"'
