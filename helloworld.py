import os
name = "Jithendar"
full_name = name + " Dharmapuri"
age = 29
good_name = "Jithu"
print(full_name)
print('my name is ' + full_name + ' people call me as ' + good_name)
print('Hello, ' + os.getlogin() + '! How are you?')
print(f'my age is ' + str(age))

fname = "Jithendar"
sname = "Dharmapuri"
if fname == "abc":
    print(f'my first name is: ' + fname)
elif sname != "Dharmapuri":
    print(f'my second name is: ' + sname)
else:
    print('my full name is ' + full_name)


DevOps_list = ["terraform", "ansible", "jenkins", "docker", "kubernetes", "ELK"]
for tools in DevOps_list:
    print(tools)
    if tools == "docker":
        break
counter = 1
while counter < 20:
  print(counter)
  if counter == 40:
    break
  counter += 1

DevOps_Set = {"terraform", "ansible", "jenkins", "docker", "kubernetes"}
print("jenkins" in DevOps_Set)
DevOps_Set.add("Prometheus")
DevOps_Set.update(DevOps_list)
print(DevOps_Set)
for tools in DevOps_Set:
    print(tools)

value = (10 + 5 * 25 / 2 - 8 % 1) #72.5
print(100 + value)
print(value - 100)
print (value)
print(type(value))

user_name = input("enter your name: ")
    #if name in user_name == user_name:
print('user name is: ' + user_name)