### Refactoring note

Having load/query modules separately is a design we made at the start of this project, 
it helps us to separate responsibility of development and eliminate 
mutual interference at the very beginning, but this also inevitably introduced 
complexity of usage and duplication in code.

Since we have entered another phase of this project, I think it's time to rethink the project
structure and combine the two modules into one.
