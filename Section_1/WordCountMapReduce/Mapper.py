# file = open('data.txt')
# data = file.read()
# print(data)
# file.close()

def mapperFunction():
    mapping = {}

    with open('data.txt') as file:
        data = file.read()

    wordsList = data.split(",")
    print(wordsList)

    for word in wordsList:
        yield word











