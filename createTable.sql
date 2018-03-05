CREATE TABLE IF NOT EXISTS lab6_rooms (
    RoomCode char(5) PRIMARY KEY,
    RoomName varchar(30) NOT NULL,
    Beds int(11) NOT NULL,
    bedType varchar(8) NOT NULL,
    maxOcc int(11) NOT NULL,
    basePrice DECIMAL(6,2) NOT NULL,
    decor varchar(20) NOT NULL,
    UNIQUE (RoomName)
);

CREATE TABLE IF NOT EXISTS lab6_reservations (
    CODE int(11) PRIMARY KEY,
    Room char(5) NOT NULL,
    CheckIn date NOT NULL,
    Checkout date NOT NULL,
    Rate DECIMAL(6,2) NOT NULL,
    LastName varchar(15) NOT NULL,
    FirstName varchar(15) NOT NULL,
    Adults int(11) NOT NULL,
    Kids int(11) NOT NULL,
    UNIQUE (Room, CheckIn),
    UNIQUE (Room, Checkout),
    FOREIGN KEY (Room) REFERENCES lab6_rooms (RoomCode)
);

INSERT INTO lab6_rooms SELECT * FROM INN.rooms;
INSERT INTO lab6_reservations SELECT CODE, Room,
DATE_ADD(CheckIn, INTERVAL 8 YEAR),
DATE_ADD(Checkout, INTERVAL 8 YEAR),
Rate, LastName, FirstName, Adults, Kids FROM INN.reservations;