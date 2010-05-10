
DROP TRIGGER IF EXISTS `geoname_point`;
CREATE TRIGGER geoname_point BEFORE INSERT ON geoname
    FOR EACH ROW BEGIN
		SET NEW.point = GeomFromText(CONCAT('POINT(',NEW.latitude, ' ', NEW.longitude, ')'));
    END;
    