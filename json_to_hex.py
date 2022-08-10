import binascii
def string_to_hex(s):
    s2=bytearray(s,'utf8')
    return binascii.b2a_hex(s2)

def hex_to_string(s):
    return binascii.a2b_hex(s)

def json_to_hex(s):
    return (string_to_hex(s)).decode()

def hex_to_json(s):
    return hex_to_string(s)

