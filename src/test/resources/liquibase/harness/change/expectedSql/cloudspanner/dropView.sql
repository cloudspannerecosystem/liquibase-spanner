CREATE VIEW test_view SQL SECURITY INVOKER AS select authors.id, authors.first_name, authors.last_name, authors.email from authors
DROP VIEW test_view