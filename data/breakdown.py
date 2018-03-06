with open('subs-raw.txt', 'r') as f:
    content = f.readlines()

subs = []
for line in content:
    subs.append(line.split()[1].split("/")[2])

print(subs)
