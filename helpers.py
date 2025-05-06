def is_arabic(query) :
    return any('\u0600' <= char <= '\u06FF' for char in query) or any('\u0750' <= char <= '\u077F' for char in query)