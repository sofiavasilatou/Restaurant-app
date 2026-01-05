from app3 import Database  # your main file name here

db = Database()


db.setpasswords(1, "sofia123")     # waiter_id = 1
db.setpasswords(2, "anastasia123")   # waiter_id = 2
db.setpasswords(3, "takis123")   # waiter_id = 3
db.setpasswords(4, "eleni123")   # waiter_id = 4
db.setpasswords(5, "petros123")   # waiter_id = 5

print("Passwords updated.")