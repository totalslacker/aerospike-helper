# LargeList

This is an implementation of a LargeList that uses standard records. It provides a one-to-many relationship. 

Consider the following scenario:

A customer holds an account with a strockbroking firm. The account can have zero or more holdings associated with it. An account has a zero-to-many relationship with holding, and reflects the account holders market position.
![One to many](OneToMany.png)

The individual elements of the the LargeList are stored as separate records using a compound key. Foe example, if the primary key of the account record is the account number `1985672`, the compound key of the element containing GOOG (an account position of Google stocks) would be `1985671::GOOG`.

The internal implementation of LargeList is responsible for creating a compound primary key for the element when it is added to the collection. It also adds the digest of the element's key to a standard CDT list in the main record. This list is used to maintain the collection.

## API


## Example
