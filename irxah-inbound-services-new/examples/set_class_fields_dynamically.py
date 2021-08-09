import typing as t

var_dict = {
    "first_name": "first name",  # this is a simple assignment
    "last_name": "last name",
    "position": "2,20",  # this needs to be interpreted
    "not_there": "?",  # this doesn't exist
}


# VERSION 0 - class and everything is assigned explicitly. kinda
class MyClass0:
    def __init__(self, **kwargs):
        self.first_name = kwargs["first_name"]
        self.last_name = kwargs["last_name"]
        self.position = kwargs["position"].split(",")  # special handling
        # not there does not get assigned

    def print_greeting(self):
        print(f"0. Hello, {self.first_name} {self.last_name}")


# usage - unpack var_dict
c = MyClass0(
    first_name=var_dict["first_name"],
    last_name=var_dict["last_name"],
    position=var_dict["position"],
)
# or better yet - unpack var_dict
c = MyClass0(**var_dict)
c.print_greeting()
# print("MyClass0().__dict__", c.__dict__)


# VERSION 1 - let's make it a class, gobble all var_dict and figure out usage later
class MyClass1:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        # maybe problems:
        # maybe-problem 1 - self.position is a string
        # maybe-problem 2 - self.not_there ... is there

    def print_greeting(self):
        # might throw error if the field variable was not passed in,
        # for example, self.foo would throw
        print(f"1. Hello, {self.first_name} {self.last_name}")


# usage
c = MyClass1(**var_dict)
c.print_greeting()
# print("MyClass1().__dict__", c.__dict__)


# VERSION 2 - make it a class, but only assign defined variables
class MyClass2:
    # define class-level variables aka attributes; these are in __annotations__
    _first_name: str  # underscore tells the outside world this is a "private" class attribute
    _last_name: str
    _position: t.Tuple[int, int]

    def __init__(self, **kwargs):
        for k in kwargs:
            # look in __annotations__ for class-level attributes without initialization value
            # and in __dict__ for un-annotated but initialized class attributes
            if f"_{k}" in MyClass2.__annotations__ or f"_{k}" in MyClass2.__dict__:
                self.__dict__[f"_{k}"] = kwargs[k]
            else:
                print(f"_{k} is not in __dict__ or __annotations__")
        # maybe - problem - self.position is string, not Tuple

    def print_greeting(self):
        print(f"2. Hello, {self._first_name} {self._last_name}")


# usage
c = MyClass2(**var_dict)
c.print_greeting()
# print("MyClass2().__dict__", c.__dict__)

# VERSION 4 - aka, "maybe this should have just been a simple dictionary"
def print_greeting4(some_dictionary: t.Dict[str, str]):
    # method knows what keys to use
    print(f"4. Hello, {some_dictionary['first_name']} {some_dictionary['last_name']}")


# usage:
print_greeting4(var_dict)


# VERSION 5 - aka "maybe I want a dictionary, method params... and a cake"
def print_greeting5(**kwargs):
    # method knows what keys to use
    print(f"5. Hello, {kwargs['first_name']} {kwargs['last_name']}")


# call with named arguments:
print_greeting5(first_name="arg1", last_name="arg2", position=[1, 5])

# usage: force-unpack dictionary
print_greeting5(**var_dict)
