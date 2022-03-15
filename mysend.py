import json
import requests
from urllib.parse import urljoin

# baseUrl = "http://10.2.110.61/api/"
baseUrl = "http://localhost:8080/api/"
filename = "new_commands.json"


def postDataToServer(data):
    uuid, author, message, likes = data
    data = {'uuid': uuid, 'author': author, 'message': message, 'likes': likes}
    r = requests.post(urljoin(baseUrl, 'messages'), json=data)
    return (r.status_code == 201, r.text)


def putDataToServer(data):
    uuid = data['u']
    del data['u']
    if 'a' in data:
        data['author'] = data.pop('a')
    if 'm' in data:
        data['message'] = data.pop('m')
    if 'l' in data:
        data['likes'] = data.pop('l')
    r = requests.put(urljoin(baseUrl, f"messages/{uuid}"), json=data)
    return (r.status_code == 204, r.text)


def deleteDataToServer(data):
    r = requests.delete(urljoin(baseUrl, f"messages/{data}"), json=data)
    return (r.status_code == 204, r.text)


def main():
    jsonFile = open(filename)
    reader = json.load(jsonFile)
    rowCounter = 1
    for row in reader['c']:
        retryCounter = 0
        while retryCounter < 3:
            (sendOk, returnMessage) = postDataToServer(row)
            if sendOk:
                print("COMMAND", rowCounter, "POST success!")
                break
            else:
                print("COMMAND", rowCounter,
                      "failed to POST:", returnMessage)
            retryCounter += 1
        rowCounter += 1
    for row in reader['u']:
        retryCounter = 0
        while retryCounter < 3:
            (sendOk, returnMessage) = putDataToServer(row)
            if sendOk:
                print("COMMAND", rowCounter, "PUT success!")
                break
            else:
                print("COMMAND", rowCounter,
                      "failed to PUT:", returnMessage)
            retryCounter += 1
        rowCounter += 1
    for row in reader['d']:
        retryCounter = 0
        while retryCounter < 3:
            (sendOk, returnMessage) = deleteDataToServer(row)
            if sendOk:
                print("COMMAND", rowCounter, "DELETE success!")
                break
            else:
                print("COMMAND", rowCounter,
                      "failed to DELETE:", returnMessage)
            retryCounter += 1
        rowCounter += 1
    jsonFile.close()


if __name__ == "__main__":
    # filename = input("Filename : ")
    # baseUrl = input(
    #     "Base URL with trailing / (e.g. http://10.2.110.61/api/) : ")
    main()
