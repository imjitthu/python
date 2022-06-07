import pytube
link = input('Enter The Youtube Video URL: ')
dn = pytube.YouTube(link)
dn.streams.first().download()
print('Your Video Has Been Downloaded: ', link)