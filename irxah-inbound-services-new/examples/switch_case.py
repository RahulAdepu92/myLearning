import sys

var = sys.argv[0] if len(sys.argv) else "abc"

# VARIANT 1 - plain ol' if-else
# PROS: more flexible, may allow fall-through
# CONS: more code
if var == "abc":
    print("1) var=abc")
elif var == "123":
    print("1) var=123")
elif var.startswith("0"):  # cannot quite do in a typical switch-case
    print("1) {var} starts with 0")
else:
    print(f"1) default handler for {var}")

# VARIANT 2 - dictionary
# PROS: closer to typical switch-case usage, more readable for simple cases
# CONS: less flexible, no fall-through
values = {
    "abc": "2) var=abc",
    "123": "2) var=123",
    "2": "2) var=0",
    "000": "2) var is all zeroes",
    "007": "2) Bond. James Bond",
}
match = values.get(var, f"2) {var} matches no keys: {values.keys()}")
print(match)
