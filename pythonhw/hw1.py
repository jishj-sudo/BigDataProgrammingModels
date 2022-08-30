import re

def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    return re.split("\t+",mystring);       

def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """
    split_mystring = q1(mystring);
    return split_mystring[1];


def q3(myarray):
    """ myarray is an list of pairs. this function should return the sum of the first
    items in the pair and the sum of the second items """
    first,second = 0,0;
    for elem in myarray:
        first += elem[0];
        second += elem[1];
    return (first , second);

def q4(mystringarray):
    """ return the position of the first occurrence of the string 'hi' or -1 if it is not found.
    you cannot change how the array is iterated and you cannot use any list operations on mystringarray"""

    for mystring in mystringarray:
        pass


def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    pass
