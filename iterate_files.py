#####to iterate lines

def million():
    with open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/good_rec.txt','r') as raw_file,open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/testgood.txt','a') as output_file:
        temp=raw_file.read()
        
        for x in range(1000):
            output_file.write(temp)
            output_file.write("\n")
       
million()



#####to append header and trialer for above created lines

def million():
    with open('C:/Users/ag20492/Desktop/ACCUMHUB/srcfiles/MVP2/testgood.txt','r') as thousand,open('C:/Users/ag65840/Music/header.txt','r') as header,open('C:/Users/ag65840/Music/trailer.txt','r') as trailer, open('C:/Users/ag65840/Music/ACCDLYINT_TST_BCI_200227102533-thousand.txt','a') as output_file:
        readhead=header.read()
        temp1="/n"
        readtrailer=trailer.read()
        read5=thousand.read()
        #read3=test30.read()
        #read2=test40.read()
        output_file.write(readhead)
        #output_file.write(temp1)
        
        output_file.write(read5)
        #output_file.write(read3)
        #output_file.write(read2)
        
        output_file.write(readtrailer)
               
million()









