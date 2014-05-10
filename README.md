mongocsvexport
==============

Utility that produces a CSV of data stored in a MongoDB

The key feature is sub-ducuments and lists expansion.

Let's imagine we have collection which contains the following documents:

```javascript
{
  "company": "Orange",
  "departments" : [
    { "title": "IT",
      "employees": [
        {"first_name" : "Andrey",
         "last_name"  : "Plotnikov"},
        {"first_name" : "Mithun",
         "last_name"  : "Chakraborty"} 
      ]},
    { "title": "Executive",
      "employees" : [
        {"first_name": "Robert",
         "last_name" : "Hunold"}
      ]
    }
  ]
}

{
  "company": "Banana",
  "departments" : [
    { "title": "Executive",
      "employees" : [
        {"first_name": "Joe",
         "last_name" : "Black"}
      ]
    }
  ]
}

```

And then run mongocsvexport command:

```
$ mongocsvexport -d testdt -c testcoll -f company,departments.title,departments.employees.last_name
```

We will get the following output:

```
Orange,IT,Plotnikov
Orange,IT,Chakraborty
Orange,Executive,Hunold
Banana,Executive,Black
```
