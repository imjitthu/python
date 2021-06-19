# import os
# name = "Jithendar"
# full_name = name + " Dharmapuri"
# age = 29
# good_name = "Jithu"
# print(full_name)
# print('my name is ' + full_name + ' people call me as ' + good_name)
# print('Hello, ' + os.getlogin() + '! How are you?')
# print(f'my age is ' + str(age))

# fname = "Jithendar"
# sname = "Dharmapuri"
# if fname == "abc":
#     print(f'my first name is: ' + fname)
# elif sname != "Dharmapuri":
#     print(f'my second name is: ' + sname)
# else:
#     print('my full name is ' + full_name)

DevOps_tools = ["terraform", "ansible", "jenkins", "docker", "kubernetes"]
for tools in DevOps_tools:
    print(tools)
    if tools == "docker":
        break