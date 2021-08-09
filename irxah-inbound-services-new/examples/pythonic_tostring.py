import typing


class ClassOne:
    def __init__(self):
        self.prop3 = "prop 3"
        self.prop2 = "prop 2"
        self.prop_a = "prop underscore a"
        self._prop1 = "private prop"

    def __str__(self):
        # __str__ is for humans => don't show private props
        return f"prop_a is {self.prop_a}, prop2 is {self.prop2}, prop3 is {self.prop3}"

    def __repr__(self):
        # __repr__ is for machines => show all props
        props = list(self.__dict__.keys())
        props.sort()
        return "; ".join((f"{p}={self.__dict__[p]}" for p in props))


c1 = ClassOne()

print(
    f"""
Class 1:
  For humans  : {c1}
  For machines: {c1!r}"""
)

# Output:
# Class 1:
#  For humans  : prop_a is prop underscore a, prop2 is prop 2, prop3 is prop 3
#  For machines: _prop1=private prop; prop2=prop 2; prop3=prop 3; prop_a=prop underscore a


# named tuples:
class ClassTwo(typing.NamedTuple):
    prop3: str = ""
    prop2: str = ""
    prop_a: str = ""
    prop1: str = ""  # cannot have "private" properties in a named tuple


c2 = ClassTwo(prop1="public prop 1", prop2="prop two", prop3="prop three", prop_a="prop A")

print(
    f"""
Class 2 (without __str__ and __repr__)
  For humans  : {c2}
  For machines: {c2!r}"""
)

# Output:
# Class 2 (without __str__ and __repr__)
#  For humans  : ClassTwo(prop3='prop three', prop2='prop two', prop_a='prop A', prop1='public prop 1')
# For machines: ClassTwo(prop3='prop three', prop2='prop two', prop_a='prop A', prop1='public prop 1')


class ClassThree(typing.NamedTuple):
    prop3: str = ""
    prop2: str = ""
    prop_a: str = ""
    prop1: str = ""  # cannot have "private" properties in a named tuple

    def __repr__(self):
        # __repr__ is for machines => show all props
        props = list(self._fields)
        props.sort()
        return "; ".join((f"{p}={getattr(self, p)}" for p in props))


c3 = ClassThree(prop1="public prop 1", prop2="prop two", prop3="prop three", prop_a="prop A")

print(
    f"""
Class 3 (with __str__ and __repr__)
  For humans  : {c3}
  For machines: {c3!r}"""
)

# Output: (notice __str__ calls into __repr__ if missing)
# Class 3 (with __str__ and __repr__)
#   For humans  : prop1=public prop 1; prop2=prop two; prop3=prop three; prop_a=prop A
#   For machines: prop1=public prop 1; prop2=prop two; prop3=prop three; prop_a=prop A
