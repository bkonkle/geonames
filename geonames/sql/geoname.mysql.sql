DROP FUNCTION IF EXISTS fn_distance_cosine_km;
DELIMITER |
CREATE FUNCTION fn_distance_cosine (
    point1 POINT,
    point2 POINT
)
RETURNS DOUBLE
BEGIN
    DECLARE latitude_1, longitude_1, latitude_2, longitude_2 DOUBLE;
    SET latitude_1:=X(point1);
    SET longitude_1:=Y(point1);
    SET latitude_2:=X(point2);
    SET longitude_2:=Y(point2);
    RETURN ACOS(
          SIN(RADIANS(latitude_1)) * SIN(RADIANS(latitude_2))
          + COS(RADIANS(latitude_1)) * COS(RADIANS(latitude_2))
          * COS(RADIANS(longitude_2 - longitude_1))
        ) * 6371;
END
|
DELIMITER ;