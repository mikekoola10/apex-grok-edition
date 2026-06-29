import os

with open("main.go", "r") as f:
    content = f.read()

# Fix JS toggleSpeech logic
# current: if(recording) recognition.start(); else recognition.stop();
# fix: if(recording) recognition.stop(); else recognition.start();
content = content.replace("if(recording) recognition.start();\\n                else recognition.stop();", "if(recording) recognition.stop();\\n                else recognition.start();")
content = content.replace("if(recording) recognition.start();\n                else recognition.stop();", "if(recording) recognition.stop();\n                else recognition.start();")

# Fix Solidity syntax
content = content.replace("import \"@openzeppelin/contracts/access/Ownable.go\";", "import \"@openzeppelin/contracts/access/Ownable.sol\";")
content = content.replace("func safeMint(address to) public onlyOwner {", "function safeMint(address to) public onlyOwner {")

with open("main.go", "w") as f:
    f.write(content)
