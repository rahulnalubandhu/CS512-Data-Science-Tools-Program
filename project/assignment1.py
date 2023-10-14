# Name : Rahul Kumar Nalubandhu

from pickle import NONE


def geq(a,b): 
    if a >= b:
        return True
    else:
        return False

def logical_mess(a, b, c):
    if None in [a,b,c]:
        return -1
    elif a == b or a == c or b == c:
        return 1
    elif a == b == c:
        return 2
    elif a > b and b > c:
        return 3
    elif c == 10 and a != 5:
        return 4
    elif c == 10 and a == 5:
        return 5
    else:
        return 6

#  def __init__(self, variable):

#         self.variable = variable

#         self.empty_array = []
 
#         self.default_integer = 0

class Adventurer:
    gold = int #an integer that describes how much gold the adventurer has
    name = str #name - a string representing the adventurer's name.
    amount = int
    item = str

    #Constructor ( str, int)
    def __init__(self, name, gold):
        self.gold = max(gold, 0)
        self.inventory = [] #a list of items represented as strings in the adventurers inventory
        self.name = name 
        
    def lose_gold(self, amount):
        if self.gold - amount < 0:
            self.gold = 0
            return False
        else:
            self.gold -= amount
            return True
    
    def win_gold(self, amount):
        self.gold += amount
       
        #variable = []
        # self.variable.append(object) and
        # self.variable.remove(object) are the 
        # handlers for adding and removing objects from an array of python objects.
    
    def add_inventory(self, item):
        self.inventory.append(item)
        
    def remove_inventory(self, item):
        if item in self.inventory:
            self.inventory.remove(item)
            return True
        else:
            return False 
            #If the Adventurer's gold - loss is less than zero,
            #then the character has 0 gold and the boolean return value False is returned from the method.