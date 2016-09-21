--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.4
-- Dumped by pg_dump version 9.5.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: test_vocab; Type: TABLE DATA; Schema: public; Owner: test_admin
--

COPY test_vocab (id, word) FROM stdin;
1	22222
2	a
3	employees
4	social
5	security
6	number
7	omb
8	no
9	15450008
\.


--
-- Name: test_vocab_id_seq; Type: SEQUENCE SET; Schema: public; Owner: test_admin
--

SELECT pg_catalog.setval('test_vocab_id_seq', 9, true);


--
-- PostgreSQL database dump complete
--
