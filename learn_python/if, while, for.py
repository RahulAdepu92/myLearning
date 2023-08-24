##if statement##

#a boolean_expression returns only TRUE/FALSE (ex: a=2,b=3  --> boolean_expression1 = a < b, boolean_expression1 = a > b)
a= 5
b =2
if a==b:
    print("")
elif a<b:
    print("")
else:
    print("")


##while loop##	

#whileloop will be executed untill the expression beside while statement becomes FALSE

#Ex1: if index greater than 5 print done

i= 1
while i<=5:
    print(i)
    i += 1
print("done")    #after loop execution is finished we are writing print statement here.

#Ex2: break the loop when quit is the command
 
command = ""
is_started = FALSE
while command != "quit":   # command != "quit" can be replaced by TRUE i.e, while TRUE: 
 command = input("> ").lower()
 if command == 'start':
    if is_started:             #this will consider as TRUE statemnt only
        print("car already started..no need to start again..ready to go")
    else:
        is_started = TRUE
        print("car started..ready to go")
 elif command == 'stop':
  print("car stopped")
 elif command == 'help':
  print("""
start - to start the car
stop - to stop the car
quit - to exit
        """)
 elif command == 'quit':
  break
 else:
  print("i dont understand")
  
  
##for loop##

#Ex1: if index greater than 5 print done

product_prices = [10,20,30]
total_bill = 0
for x in product_prices:
    total_bill += x
print(f"Your bill is: {total_bill}")

"""
refernce links:

https://www.w3schools.com/python/python_while_loops.asp
https://www.programiz.com/python-programming/for-loop
"""

