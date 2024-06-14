CREATE TABLE Strzelanina (
  id INTEGER PRIMARY KEY,
  victims_killed INTEGER,
  victims_injured INTEGER,
  suspects_killed INTEGER,
  suspects_injured INTEGER,
  suspects_arrested INTEGER
);

CREATE TABLE Pogoda (
  id INTEGER PRIMARY KEY,
  strzelanina_id INTEGER REFERENCES Strzelanina,
  temperatura DECIMAL(2, 1),
  chmury INTEGER,
  opady INTEGER,
  cisnienie INTEGER,
  wiatr_predkosc INTEGER,
  wiatr_kierunek INTEGER
);


CREATE TABLE Pogoda (
  id INTEGER PRIMARY KEY
  strzelanina_id INTEGER REFERENCES Strzelanina,
  temperatura DECIMAL(2, 1),
  chmury INTEGER,
  opady INTEGER,
  cisnienie INTEGER,
  wiatr_predkosc INTEGER,
  wiatr_kierunek INTEGER
);


CREATE TABLE Lokalizacja (
  id INTEGER PRIMARY KEY,
  strzelanina_id INTEGER REFERENCES Strzelanina,
  state VARCHAR(255),
  city VARCHAR(255),
  address VARCHAR(255),
);

CREATE TABLE Dostep_do_broni (
  id INTEGER PRIMARY KEY,
  strzelanina_id INTEGER REFERENCES Strzelanina,
  lokalizacja VARCHAR(255),
  rok INTEGER,
  dostep_do_broni BOOLEAN,
);

CREATE TABLE Czas (
  id INTEGER PRIMARY KEY,
  strzelanina_id INTEGER REFERENCES Strzelanina,
  dekada INTEGER,
  miesiac INTEGER,
  dzien INTEGER,
);