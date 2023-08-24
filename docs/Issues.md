# Issues


## 1. CPython C extensions field assignments

So with CPython C extensions you can define a type that has some fields/members that it defines getters/setters for:

[2. Defining Extension Types: Tutorial — Python 3.11.4 documentation](https://docs.python.org/3/extending/newtypes_tutorial.html#providing-finer-control-over-data-attributes)

See `Custom_setfirst`.  This is performing an assignment in C to a field.  I don't see how we can intercept this with a write barrier.  Hence, without care this is going to be a breaking change.

 
### Solutions

#### Explicitly mark types as regionable
I think we could address this in the long term by adding a feature to 

`.tp_flags`

that specifies this class works correctly with regions. E.g. there are things like

```
.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
```

`Py_TPFLAGS_REGION_COMPATIBLE` that requires field assignment calls a write barrier.

#### Wrap all field assignments

Wrap the setter with the write barrier code.