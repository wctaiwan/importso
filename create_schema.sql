CREATE TABLE question (
	question_id INTEGER PRIMARY KEY,
	creation_date INTEGER,
	title TEXT,
	body TEXT,
	tags TEXT,
	score INTEGER,
	accepted_answer_id INTEGER
);

CREATE TABLE answer (
	answer_id INTEGER PRIMARY KEY,
	creation_date INTEGER,
	question_id INTEGER,
	body TEXT,
	score INTEGER
);
