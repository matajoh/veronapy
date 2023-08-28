/*****************************************************************************
 * This file contains a model implementation of region tracking.
 * 
 * It uses a union-find data structure to track the regions, and efficiently
 * merge them when necessary. 
 *
 * TODO track uniqueness of regions - currently this allows the same region
 * to be assigned to multiple objects, and hence breaks its uniqueness. 
 */


#include <map>
#include <string>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <memory>

class UnionFind
{
private:
  // Bottom two bits are used for status
  //   00 - parent object
  //   1X - region root
  //     10 - implicit region root
  //     11 - explicit region root
  // For region root, the remaining bits represent the max depth of the 
  // region parent tree.
  // For parent object, the remaining bits represent the aligned pointer to the parent object. 
  uintptr_t parent;

  bool is_root() const { return (parent & 0x3) != 0; }
  bool is_explicit() const { return (parent & 0x3) == 0x3; }

  UnionFind* get_parent() const
  {
    assert(!is_root());
    return (UnionFind*)parent;
  }

  size_t get_depth() const
  {
    assert(is_root());
    return parent >> 2;
  }

protected:
  void set_explicit()
  {
    assert(is_root());
    parent |= 0x3;
  }

  // Default to an implicit region root.
  UnionFind() : parent(2) {}

  UnionFind* find()
  {
    if (is_root())
      return this;
  
    // Perform grandparent path compression to reduce the depth of the parent
    // pointing tree.
    auto curr = this;
    auto parent = get_parent();
    while (!parent->is_root())
    {
      auto grandparent = parent->get_parent();
      curr->parent = (uintptr_t)grandparent;
      curr = parent;
      parent = grandparent;
    }
    return parent;
  }

  void merge(UnionFind* other)
  {
    UnionFind* this_root = find();
    UnionFind* other_root = other->find();
    // Equal so no need to merge.
    if (this_root == other_root)
      return;

    // Ensure that the merge is always done with the deeper tree as the parent.
    if (this_root->get_depth() < other_root->get_depth())
      std::swap(this_root, other_root);
    
    // Increase depth if the two trees are the same depth.
    if (this_root->get_depth() == other_root->get_depth())
      this_root->parent += 4; // Increment depth (by 1)

    assert(this_root->get_depth() > other_root->get_depth());

    // Determine if the merge is valid.
    if (this_root->is_explicit() && other_root->is_explicit())
    {
      std::cout << "Merging two explicit regions - abort" << std::endl;
      abort();
    }

    // Preserve explicit marker if on smaller side.
    if (other_root->is_explicit())
    {
      this_root->set_explicit();
    }

    other_root->parent = (uintptr_t)this_root;
  }
};

class Reference;

class DynObject;

class ObjectReference
{
  friend class Reference;

  std::shared_ptr<DynObject> object;

public:
  ObjectReference() : object(nullptr) {}

  ObjectReference(std::shared_ptr<DynObject> object) : object(object) {}

  ObjectReference(const ObjectReference& objref) : object(objref.object) {}

  ObjectReference(ObjectReference&& objref) : object(objref.object) {}

  ObjectReference& operator=(const ObjectReference& other)
  {
    object = other.object;
    return *this;
  }

  ObjectReference& operator=(ObjectReference&& other)
  {
    object = other.object;
    return *this;
  }

  Reference operator[](std::string name);

  static ObjectReference create()
  {
    return {std::make_shared<DynObject>()};
  }

  static ObjectReference create_region()
  {
    return {std::make_shared<DynObject>(true)};
  }
};

class DynObject : UnionFind
{
  friend class Reference;
  friend class ObjectReference;

  bool is_region;

  std::map<std::string, ObjectReference> fields;
public:
  DynObject(bool is_region = false) : is_region(is_region)
  {
    if (is_region)
      set_explicit();
  }
};

class Reference
{
  std::string key;
  DynObject* object;

public:
  Reference(std::string name, DynObject* object) : key(name), object(object) {}

  operator ObjectReference() { return object->fields[key]; }

  Reference& operator=(const ObjectReference& other)
  {
    ObjectReference& cell = object->fields[key];
    cell = other;
    if (!other.object->is_region)
      object->merge(other.object.get());
    return *this;
  }
};

Reference ObjectReference::operator[](std::string name)
{
  return Reference(name, this->object.get());
};


int main()
{
  std::cout << "region a" << std::endl;
  auto a = ObjectReference::create_region();
  std::cout << "region b" << std::endl;
  auto b = ObjectReference::create_region();
  std::cout << "object c" << std::endl;
  
  auto c = ObjectReference::create();
  std::cout << "object d" << std::endl;
  auto d = ObjectReference::create();
  std::cout << "object e" << std::endl;
  auto e = ObjectReference::create();
  std::cout << "object f" << std::endl;
  auto f = ObjectReference::create();

  // Merge implicit regions
  std::cout << "c.x = e" << std::endl;
  c["x"] = e;
  // Merge explicit and implicit
  std::cout << "a.x = c" << std::endl;
  a["x"] = c;

  // Merge implicit and explicit
  std::cout << "f.w = a" << std::endl;
  f["w"] = c;

  // Merge explicit and implicit
  std::cout << "b.y = d" << std::endl;
  b["y"] = d;

  // Should already be the same region
  std::cout << "a.y = e" << std::endl;
  a["y"] = e;

  // Nest a region
  std::cout << "a.y = b" << std::endl;
  a["y"] = b;
  
  // Should fail.
  std::cout << "a.z = d" << std::endl;
  a["z"] = d;

  return 0;
}
