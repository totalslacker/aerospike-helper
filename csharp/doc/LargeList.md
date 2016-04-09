# LargeList

This is an implementation of a LargeList that uses standard records. It provides a one-to-many relationship. 

Consider the following scenario:

A customer holds an account with a strockbroking firm. The account can have zero or more holdings associated with it. An account has a zero-to-many relationship with holding, and reflects the account holders market position.
![One to many](../../graphics/OneToMany.png)

The individual elements of the the LargeList are stored as separate records using a compound key. Foe example, if the primary key of the account record is the account number `1985672`, the compound key of the element containing GOOG (an account position of Google stocks) would be `1985671::GOOG`.

In a normal LDT list, there is a control Bin that stores LDT meta data and each element is stored in a special sub-record. 

![LDT](../../graphics/LDT.png)

In a CDT list, the elements of the list are stored contiguously in the Bin and the size of the list is limited by the maximum size of the record. Max record size is 128k by default, but can be expanded to 1M)

![CDT](../../graphics/CDT.png)

The internal implementation of LargeList is responsible for creating a compound primary key for the element when it is added to the collection. It also adds the digest of the element's key to a standard CDT list in the main record. This list is used to maintain the collection.

![HyBrid](../../graphics/HyBrid.png)

## API
The API uses the same method signatures as the LDT LargeList allowing a drop in replacement. Some methods that are available in the LDT LargeList are not implemented and will throw an `NotImplementedException` if called. 

## Example
