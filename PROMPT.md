# Example prompts for Cursor

## Support for specifying the join i.e. `LEFT` or `RIGHT`

I want to be able to specify the join type. I want to use a table modifier (see
`table-mod` in @pine.bnf ) to specify this e.g.

- Specify join types i.e. `LEFT JOIN` or `RIGHT JOIN`:
```
x | y :left
x | y :right
```

This means that the following need to be updated:
- @pine.bnf 
- @parser.clj and @parser_test.clj 
- @main.clj and @ast_test.clj 
- @eval.clj and @eval_test.clj 


## Support for dates

I want to be able to specify dates in the format `YYYY-MM-DD`.

- Specify dates in the format `YYYY-MM-DD`:
```
x | created_at > '2025-01-01' | created_at < '2026-01-01'
```

This means that the following need to be updated:
- @pine.bnf 
- @parser.clj and @parser_test.clj 
- @main.clj and @ast_test.clj 
- @eval.clj and @eval_test.clj 

## Support for `update!` operation

I want to be able to specify the `update!` operation e.g.

```
customers | where: id = 1 | update! name = 'John Doe'
```

which should be evaluated to:

```
UPDATE customers SET name = 'John Doe' WHERE id IN (SELECT id FROM customers WHERE id = 1) 
```

We will use the existing select statement and wrap that in an update statement.

This means that the following need to be updated:
- @pine.bnf 
- @parser.clj and @parser_test.clj 
- @main.clj and @ast_test.clj 
- @eval.clj and @eval_test.clj 


## Support for specifying the alias in the order operation

I want to be able to specify the alias in the order operation. I want to use a table modifier (see
`order-column` in @pine.bnf ) to specify this e.g.

Example:
```
employee as e | o: e.name desc
```

This means that the following need to be updated:
- @pine.bnf 
- @parser.clj and @parser_test.clj 
- @main.clj and @ast_test.clj 
- @eval.clj and @eval_test.clj 