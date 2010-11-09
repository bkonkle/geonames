
DELIMITER $$

DROP FUNCTION IF EXISTS `distance` $$
CREATE FUNCTION distance(a POINT, b POINT) RETURNS double
DETERMINISTIC
COMMENT 'Spatial distance function using the great-circle distance formula (in km)'
RETURN ( 6378.7
    * acos(
        sin( radians(X(a)) ) * sin( radians(X(b)) )
    + cos( radians(X(a)) ) * cos( radians(X(b)) )
* cos( radians(Y(b)) - radians(Y(a)) )
)
) $$

DELIMITER ;
