##arbitary arguments --called as *args


def my_function(*kids):
  print("The youngest child is " + kids[2])

my_function("Emil", "Tobias", "Linus")



##Arbitrary Keyword Arguments --called as  **kwargs


def my_function(**kid):
  print("His last name is " + kid["lname"])

my_function(fname = "Tobias", lname = "Refsnes")


##packages and libraries/modules

__init__.py file creation in a folder will indicate that the folder is a package and under it we will have libraries

example:

Claculation          #folder(also called as package due to presenc of init file)
	__init__.py
	addition.py
	substraction.py
	divison.py
	