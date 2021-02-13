
# Research Questions and potential queries in the WST

Overall questions: What does code look like? What are the basic statistics of code projects across a huge number of repos?

RQ1: What does a code project look like?
 - [ ] Total LOC: TODO
 - [x] Number of files:
   - number of WSTFile nodes under a WSTRepository
 - [x] Number of classes:
   - per file: `match cheese=(sauce:WSTRepository)<-[]-(pizza:WSTFile)<-[]-(toppings:WSTNode {type: "class_definition"}) return sauce.url, pizza.path, count(toppings) limit 30`
 - [x] Number of methods:
   - function definitions below a class definition: `match cheese=(sauce:WSTRepository)<-[]-(pizza:WSTFile)<-[]-(toppings:WSTNode {type: "class_definition"})<-[:PARENT*..2]-(mushroom:WSTNode {type: "function_definition"}) return sauce.url, pizza.path, toppings.x1, count(mushroom) limit 30`
 - [x] Number of different languages:
   - in WSTFile.language (only for WST recognized languages): `match pizza=(crust:WSTRepository)<-[:IN_REPO]-(cheese:WSTFile) where cheese.language is not null return distinct crust.url, cheese.language limit 30`
 - [ ] Number of GitHub stars: out-of-scope for VFP (API hits are expensive)
 - [ ] Number of contributors: TODO
 - [ ] Number of commits: TODO
 - [ ] Repo age: TODO
 - All of these should be aggregated by language (as designated by the GitHub repo?)

RQ1.5/Maybe: What is the file structure of a repo?
 - [x] Number of directories:
   - iterate through file paths in WSTFile, add the directory component to a `set` for the repo
 - [x] Number of different file extensions:
   - same approach as number of directories, but split file exts
 - [ ] Layout of file structure: TODO needs clarification
 - [x] Max directory depth:
   - iterator over file paths, if longer, replace
 - All of these should be aggregated by language (as designated by the GitHub repo?)

RQ2: What is the visual shape of code?
 - [x] Length of files:
   - Counting lines of the root node of a file: `match cheese=(pizza:WSTFile)<-[]-(toppings:WSTNode)-[]->(crust:WSTText) where not (toppings)-[:PARENT]->(:WSTNode) return pizza.path, toppings.x1, toppings.x2 limit 30`
 - [x] Length of classes:
   - Counting start and end lines of each class: `match cheese=(sauce:WSTRepository)<-[]-(pizza:WSTFile)<-[]-(toppings:WSTNode {type: "class_definition"}) return sauce.url, pizza.path, toppings.x1, toppings.x2 limit 30`
 - [x] Length of functions:
   - same idea: `match cheese=(sauce:WSTRepository)<-[]-(pizza:WSTFile)<-[]-(toppings:WSTNode {type: "function_definition"}) return sauce.url, pizza.path, toppings.x1, toppings.x2 limit 30`
 - [x] Width of functions:
   - get text from each function and find longest line: `match cheese=(sauce:WSTRepository)<-[]-(pizza:WSTFile)<-[]-(toppings:WSTNode {type: "function_definition"})-[:CONTENT]->(pepper:WSTText) return sauce.url, pizza.path, toppings.x1, toppings.x2, toppings.y1, toppings.y2, pepper.text limit 30`
 - [ ] Heatmaps showing the shape?
 - All of these should be aggregated by language (as designated by the GitHub repo?)

RQ3: What is in a line of code?
 - [ ] Comments: TODO
 - [ ] Stats on frequency and associations between token types
 - [ ] Heatmaps showing different types of tokens?

RQ4: What is the correlation between all of these results and various project factors?
 - Relationships between RQ1-3 results and...
 - Number of stars
 - LOC
 - Number of contributors
 - Repo age
 - Number of commits
 - Time since commit
