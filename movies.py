from tkinter import *

root = Tk()

D = []
file = open("/home/cloudera/result/user_ratings.txt").readlines()
R =["----------------------------------------------------------------\n"]
E = ["MOVIES RATED BY YOU! \n"]
F =R+ E+R + file
G = " ".join(F)
w = Text(root)


H = []
file2 = open("/home/cloudera/result/recommendation.txt").readlines()
Z=["-----------------------------------------------------------------\n"]
H = ["RECOMMENDED MOVIES FOR YOU: \n"]
I =Z + H + Z + file2
J = " ".join(I)
w = Text(root)

A = []
file1 = open("/home/cloudera/result/result1.txt").readlines()
X =["----------------------------------------------------------------\n"]
A = ["WATCH SOME OF THE POPULAR MOVIES ONES! : \n"]
C =X+ A + X + file1
B = " ".join(C)
N = G+"\n"+J+"\n"+B
w = Text(root, height = 50, width = 100)


w.insert(END,N)
#w = Label(root, text = N)
#w.config(height = 50,width=100)
#w.config(bg='cyan', fg='Dark blue')
w.pack(side="left", expand="yes", fill="both")
w.configure(state = 'disabled')
button_quit = Button(root, text = "Exit",command = root.quit)
button_quit.pack()
root.title("Movie Recommendation")
root.mainloop()

