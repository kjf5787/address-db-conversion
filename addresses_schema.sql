CREATE TABLE `addresses` (
	`zipcode`	VARCHAR ( 6 ) NOT NULL,
	`number`	VARCHAR ( 30 ) NOT NULL,
	`street`	VARCHAR ( 200 ) NOT NULL,
	`street2`	VARCHAR ( 20 ),
	`city`	VARCHAR ( 50 ) NOT NULL,
	`state`	CHAR ( 2 ) NOT NULL,
    `plus4`	CHAR ( 4 ),
    `country` CHAR ( 2 ) NOT NULL DEFAULT 'US',
	`latitude`	DECIMAL ( 8 , 6 ) NOT NULL,
	`longitude`	DECIMAL( 9 , 6 ) NOT NULL,
    `source`	VARCHAR( 40 ),
    UNIQUE (zipcode, number, street, street2, country)
);
CREATE INDEX `latitude_longitude` ON `addresses` (
	`latitude`,
	`longitude`
);
CREATE INDEX `number_street` ON `addresses` (
	`number`,
	`street`
);
CREATE INDEX `state_city` ON `addresses` (
	`state`,
	`city`
);
CREATE INDEX `zipcode_number` ON `addresses` (
	`zipcode`,
	`number`
);
CREATE INDEX `country` ON `addresses` (
	`country`
);
