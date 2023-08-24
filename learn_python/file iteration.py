#####to iterate lines#####

def iterate():
    with open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/detail.txt', 'r') as input_file,\
            open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/iteratedfile.txt', 'a') as output_file:    ##remove already existing iteratedfile before execution of this method
        readfile = input_file.read()

        for x in range(1000):  ##this will iterate number of records in file 1000 times
            output_file.write(readfile)
            output_file.write("\n")

iterate()



#####to append header and trialer for above created lines#####

def final():
    with open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/iteratedfile.txt', 'r') as iteratedfile,\
            open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/header.txt', 'r') as header,\
            open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/trailer.txt', 'r') as trailer,\
            open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/ACCDLYINT_TST_BCIINRX_200421065111-100k records.txt','a') as output_file:  ##change the file name here
        readheader = header.read()
        readtrailer = trailer.read()
        readiteratedfile = iteratedfile.read()

        output_file.write(readheader)
        output_file.write("\n")  # temp1="/n"
        output_file.write(readiteratedfile)
        output_file.write("\n")  # temp1="/n"
        output_file.write(readtrailer)
        
final()

#####to print and count number of lines in a file#####

file = open(r"C:\Users\ag20492\Desktop\ACCUMHUB\srcfiles\MVP2\ACCDLYINT_TST_BCIINRX_200421065111-1k records.txt",'r')
print(file.read())			   #reads the entire file and displays all lines
print(file.read(1000))		   #displays first 1000 characters
print(file.readline())         #displays first line
print(file.readlines())		   #reads the entire file line by line and displays all lines
print(len(file.readlines()))   #gives the total number of rows or lines

file.seek(0)                   #to close the above opened file. 
                               #Everytime you open a file and execute some task, close it using this command. Else, go by 'with' statement that automatically closes the opened file.




#Instead you can write:
with open(r"C:\Users\ag20492\Desktop\ACCUMHUB\srcfiles\MVP2\ACCDLYINT_TST_BCIINRX_200421065111-1k records.txt",'r') as file:
    print(len(file.readlines()))


#####to check row level duplicates#####

with open(r"C:\Users\ag20492\Desktop\ACCUMHUB\srcfiles\MVP2\check_duplicate.txt",'r') as file:
     seen = set()
     for line in file:
         if line in seen:
             print(line)
         else:
             seen.add(line)
