#---------------------------------------------------------------------------------------------
# Name: NHD_to_WKT.r
# Purpose: converts the NHD flowlines to a WKT for inclusion in a database.
# Author: Christopher Tracey
# Created: 2018-11-07
# Updated: 2019-05-14
#---------------------------------------------------------------------------------------------

if (!requireNamespace("here", quietly=TRUE)) install.packages("here")
require(here)
if (!requireNamespace("sf", quietly=TRUE)) install.packages("sf")
require(sf)
if (!requireNamespace("arcgisbinding", quietly=TRUE)) install.packages("arcgisbinding")
require(arcgisbinding)

arc.check_product()

# variables
gdb_boundaries <- "NHDPlusV21_National_Seamless_PROJECTED.gdb"
flowlines <- "NHDFlowline_Network_projected"
nhdArea <- "CONUS_AreaWaterbody_new"

##########################################################
# get the NHD flowlines
flowlines_shp <- arc.open(here(gdb_boundaries,flowlines))
flowlines_shp <- arc.select(flowlines_shp, fields=c("COMID"))
flowlines_sf <- arc.data2sf(flowlines_shp)
rm(flowlines_shp)

#check geometry
flowlines_sf_1 <- st_zm(flowlines_sf)
geochk_flowlines <- st_is_valid(flowlines_sf_1)
flowlines_sf_1 <- cbind(flowlines_sf_1,geochk_flowlines)

# export the linework as a wkt file
flowlines_wkt <- st_as_text(flowlines_sf$geom)
# join back to the COMIDs
flowlines_wkt1 <- as.data.frame(cbind(flowlines_sf$COMID,flowlines_wkt))
colnames(flowlines_wkt1) <- c("COMID", "wkt")
# add in the huc12s
huc12 <- read.csv("huc12.csv", stringsAsFactors=FALSE, colClasses=c("HUC_12"="character"))
flowlines_wkt1 <- merge(flowlines_wkt1,huc12,by="COMID", all.x=TRUE)
flowlines_wkt1$X <- NULL
flowlines_wkt1 <- flowlines_wkt1[c("COMID","HUC12","wkt")]
write.csv(flowlines_wkt1, "background.csv",row.names=FALSE)

# get the projection string
st_crs(flowlines_sf)$proj4string.

##########################################################
# HUC12
huc12 <- "huc12_projected"

huc12_shp <- arc.open(here(gdb_boundaries,huc12))
huc12_shp <- arc.select(huc12_shp, fields=c("HUC_12"))
huc12_sf <- arc.data2sf(huc12_shp)
rm(huc12_shp)

#check geometry
huc12_sf_1 <- st_zm(huc12_sf)
geochk_huc12 <- st_is_valid(huc12_sf_1)
huc12_sf_1 <- cbind(huc12_sf_1,geochk_huc12)
hucErrors <- huc12_sf_1[which(huc12_sf_1$geochk_huc12==FALSE),]

# export the linework as a wkt file
huc12_wkt <- st_as_text(huc12_sf$geom)
huc12_table <- as.data.frame.matrix(cbind(huc12_sf$HUC_12,huc12_wkt))
colnames(huc12_table) <- c("HUC12","wkt")
write.csv(huc12_table, "huc12.csv", row.names=FALSE)


# ones based on the exploded multipart polygons
huc12a <- "HUC12_SpatialIssues"

huc12a_shp <- arc.open("E:/MoBI/StreamCat/spatial_data/wkt_conversion.gdb/HUC12_projected_MultipartToS")
huc12a_shp <- arc.select(huc12a_shp, fields=c("HUC_12"))
huc12a_sf <- arc.data2sf(huc12a_shp)
#check geometry
huc12a_sf_1 <- st_zm(huc12a_sf)
geochk_huc12a <- st_is_valid(huc12a_sf_1)
huc12a_sf_1 <- cbind(huc12a_sf_1,geochk_huc12a)
hucErrors_a <- huc12a_sf_1[which(huc12a_sf_1$geochk_huc12==FALSE),]

huc12a_wkt <- st_as_text(huc12a_sf$geom)

# export the linework as a wkt file
huc12a_table <- as.data.frame.matrix(cbind(huc12a_sf$HUC_12,huc12a_wkt))
colnames(huc12a_table) <- c("HUC12","wkt")
write.csv(huc12a_table, "huc12_exploded.csv", row.names=FALSE)




##########################################################
# get the NHD area
nhdArea_shp <- arc.open(here(gdb_boundaries,nhdArea))
nhdArea_shp <- arc.select(nhdArea_shp)
nhdArea_sf <- arc.data2sf(nhdArea_shp)

#check geometry
geochk <- st_is_valid(nhdArea_sf)


rm(nhdArea_shp)
# export the linework as a wkt file
nhdArea_wkt <- st_as_text(nhdArea_sf$geom)

nhdArea_COMID <- nhdArea_sf$COMID

nhdArea_table <- cbind(nhdArea_COMID,nhdArea_wkt)
nhdArea_table1 <- as.data.frame(nhdArea_table)


colnames(nhdArea_table1) <- c("COMID","wkt")

nhdArea_table1a <- merge(nhdArea_table1,nhdArea_sf[c("COMID","FTYPE","FCODE")],by="COMID")
nhdArea_table1a$geom <- NULL



write.csv(nhdArea_table1a,"nhdArea.csv", row.names=FALSE)

# get the projection string
st_crs(nhdArea_sf)$proj4string

