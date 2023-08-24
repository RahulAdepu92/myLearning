##object is an instance of a class
##function becomes a method when included in a class
##object must have atleast one argument (by default 'self' will be present), whereas function dont essentially requiere an argument 
##By using the “self” keyword we can access the attributes and methods of the class in python

#Example1:

class Point:    #point is the class; starting letter of class should be capital
    def move(self):  #self is a parameter here generated automatically
        print("move")
    
    def draw(self):
        print("draw")
    
    def paint(self):
        print("paint")
    
point1 = Point()   #point1 is the object

point1.move()  #move is the method
#>>move

point1.draw()   #point1 is the object
#>>draw    		#draw is the method


point2 = Point()   #point2 is another object
point2.paint()  #paint is the method
#>>paint


## __init__ (called as initialization) is a method and is considered a constructor

#Example2:

class Person:
    def __init__(self, name):
        self.name = name
        
    def talk(self):
        print(f"hi {self.name}")
        
rahul = Person("Rahul Adepu")
print(rahul.name)
#>>Rahul Adepu

rahul.talk()
#>>hi Rahul Adepu

#Example3:

class Vehicle(object):
    """docstring"""

    def __init__(self, color, doors, tires):
        """Constructor"""
        self.color = color   #attribute 1
        self.doors = doors	 #attribute 2
        self.tires = tires   #attribute 3

    def brake(self):
        """
        Stop the car
        """
        return "Braking"

    def drive(self):
        """
        Drive the car
        """
        return "I'm driving!"
        
        
model = Vehicle("blue", "2", "4")
print(model.color)
#>>blue
print(model.brake)


##############

class Subject:
    def __init__(self, name):
        self.name = name

    def science(self):
        print(f"the subject is: {self.name}")

subject1 = Subject("physics")
subject1.science()
