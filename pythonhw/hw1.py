
<<<<<<< HEAD
def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    return mystring.split("\t");       

def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """
    split_mystring = q1(mystring);
    return split_mystring[1];
=======

def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    pass


def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """
    pass

>>>>>>> 48148a52da64c36ecbbefd2262446d9cd3171795


def q3(myarray):
    """ myarray is an list of pairs. this function should return the sum of the first
    items in the pair and the sum of the second items """
<<<<<<< HEAD
    first,second = 0,0;
    for elem in myarray:
        first += elem[0];
        second += elem[1];
    return (first , second);
=======
    pass

>>>>>>> 48148a52da64c36ecbbefd2262446d9cd3171795

def q4(mystringarray):
    """ return the position of the first occurrence of the string 'hi' or -1 if it is not found.
    you cannot change how the array is iterated and you cannot use any list operations on mystringarray"""
<<<<<<< HEAD
    position = 0;
    for mystring in mystringarray:
        if(mystring == "hi"):
            return position;
        position += 1;
    return -1;
    

def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    dic = {};
    
    for letter in myarray:
        if( letter in dic ):
            dic[letter] += 1;
        else:    
            dic[letter] = 1;
    return dic;   
=======

    for mystring in mystringarray:
        pass


def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    pass
>>>>>>> 48148a52da64c36ecbbefd2262446d9cd3171795
