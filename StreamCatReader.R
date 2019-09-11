library(plyr)
library(here)
library(RSQLite)
library(reshape)
library(Hmisc)
library(RCurl)
if (!requireNamespace("arcgisbinding", quietly=TRUE)) install.packages("arcgisbinding")
require(arcgisbinding)
if (!requireNamespace("foreign", quietly=TRUE)) install.packages("foreigng")
require(foreign) # for reading dbf files
library(igraph)

arc.check_product() # for the arcgis-r bridge

setwd(here::here("data"))


memory.limit(size=56000) # otherwise we have vector size problems

# geodatabase name and layers
NHDgdb <- "NHDPlusV21_National_Seamless.gdb"
flowlines <- "NHDSnapshot/NHDFlowline_Network"
waterbodies <- "NHDSnapshot/NHDWaterbody"
area <- "NHDSnapshot/NHDArea"

##################################################################################################################################
#  get a list of reaches from the NHDplus v2
NHDflowlines <- arc.open(here("spatial_data",NHDgdb,flowlines))
NHDflowlines <- arc.select(NHDflowlines)
NHDflowlines <- as.data.frame(NHDflowlines)

##################################################################################################################################
# get NHD v1 to v2 crosswalk
crosswalk <- read.dbf(here("data/crosswalk","NHDPlusV1Network_V2Network_Crosswalk.dbf"),as.is=FALSE)

##################################################################################################################################
# build new NHD base dataset
NHD_MoBI <- NHDflowlines[c("COMID","GNIS_ID","GNIS_NAME","LENGTHKM","REACHCODE","WBAREACOMI","FTYPE","FCODE","StreamLeve","StreamOrde","StreamCalc","SLOPE","Tidal","MAXELEVSMO","MINELEVSMO","ELEVFIXED","WBAreaType","VC_MA","VA_MA","QC_MA","VC_MA","QE_MA","VE_MA","VC_01","VA_01","QC_01","VC_01","QE_01","VE_01","VC_02","VA_02","QC_02","VC_02","QE_02","VE_02","VC_03","VA_03","QC_03","VC_03","QE_03","VE_03","VC_04","VA_04","QC_04","VC_04","QE_04","VE_04","VC_05","VA_05","QC_05","VC_05","QE_05","VE_05","VC_06","VA_06","QC_06","VC_06","QE_06","VE_06","VC_07","VA_07","QC_07","VC_07","QE_07","VE_07","VC_08","VA_08","QC_08","VC_08","QE_08","VE_08","VC_09","VA_09","QC_09","VC_09","QE_09","VE_09","VC_10","VA_10","QC_10","VC_10","QE_10","VE_10","VC_11","VA_11","QC_11","VC_11","QE_11","VE_11","VC_12","VA_12","QC_12","VC_12","QE_12","VE_12")]

# merge the crosswalk intot he NHD_MoBI file so we join the NHD+v1 attributes
NHD_MoBI1 <- merge(NHD_MoBI, crosswalk[c("V1_ComID","V1RchCode","V1Ftype","V2_ComID","V2RchCode","V2Ftype","XWalkType")], by.x="COMID", by.y="V2_ComID", all=TRUE)  # may want to switch to an inner join.
rm(NHDflowlines)

##################################################################################################################################
# NHDplus version 1 attributes
NHDv1attrlist <- list.files(path=here::here("data/v1_NHD_attributes"), pattern="*.zip", full.names=TRUE)
ldply(.data=NHDv1attrlist, .fun=unzip, exdir=here::here("data/v1_NHD_attributes"))

# manually rename and sort, as I can't seem to figure this out now...

# make a list of the table types
tabletypes <- c("catchmentattributesnlcd","catchmentattributestempprecip","flowlineattributesflow","flowlineattributesnlcd","flowlineattributestempprecip") # not doing "headwaternodearea" as its not relevant for this project
for(p in 1:length(tabletypes)) {
  v1attrib <- list.files(path=here::here("data/v1_NHD_attributes"), pattern=paste("*",tabletypes[p],"*",sep=""), full.names=FALSE)
  for (q in 1:length(v1attrib)){  
    assign(paste("v1attrib",gsub("[.]dbf$","",v1attrib[q]),sep="_"), read.dbf(here::here("data/v1_NHD_attributes",v1attrib[q]),as.is=TRUE))
  }
  assign(paste("v1attCombined",tabletypes[p],sep="_"), do.call(rbind, mget(ls(pattern=tabletypes[p])) ) )
  print(paste(paste("v1attCombined",tabletypes[p],sep="_"),"completed"))
  rm(list=ls(pattern="v1attrib_*"))  
}
# work the individual layers
# v1attCombined_flowlineattributesflow
v1attCombined_flowlineattributesflow$GRID_CODE <- NULL #drop GRID_CODE
# v1attCombined_flowlineattributestempprecip
v1attCombined_flowlineattributestempprecip$GRID_CODE <- NULL #drop GRID_CODE
# v1attCombined_catchmentattributestempprecip
v1attCombined_catchmentattributestempprecip$GRID_CODE <- NULL #drop GRID_CODE
# v1attCombined_flowlineattributesnlcd
v1attCombined_flowlineattributesnlcd$GRID_CODE <- NULL
# v1attCombined_catchmentattributesnlcd
v1attCombined_catchmentattributesnlcd$GRID_CODE <- NULL
#  merge all the tables together
v1attributes <- merge(v1attCombined_flowlineattributesflow,v1attCombined_flowlineattributestempprecip, by="COMID")
v1attributes <- merge(v1attributes, v1attCombined_catchmentattributestempprecip, by="COMID")

# remove the excess
rm("v1attCombined_flowlineattributesnlcd","v1attCombined_catchmentattributesnlcd","v1attCombined_flowlineattributestempprecip","v1attCombined_catchmentattributestempprecip","v1attCombined_flowlineattributesflow")

##################################################################################################################################
# Lotic - streamCat

# download StreamCat data from ftp://newftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/
url = "ftp://newftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/"
filenames = getURL(url, ftp.use.epsv = FALSE, dirlistonly = TRUE)
filenames <- strsplit(filenames, "\r\n")
filenames = unlist(filenames)
filenames
for (filename in filenames) {
  download.file(paste(url, filename, sep = ""), paste(getwd(), "/", filename, sep = ""))
}
setwd(here::here())

## can we add in something to check for 0kb zip files???

# unzip files
filelist <- list.files(path=here::here("data/streamcat"), pattern="*.zip", full.names=TRUE)
ldply(.data=filelist, .fun=unzip, exdir=here::here("data"))

# read the csv files
csv_files <- list.files(path=here::here("data/streamcat"), pattern="*.csv", full.names=FALSE)

csv_groups <- gsub("_[^_]+$", '_', csv_files)
csv_groups <- unique(csv_groups)
csv_groups <- gsub(pattern="\\.csv$", "", csv_groups)

for (j in 1:length(csv_groups)){
  csv_files1 <- list.files(path=here::here("data/streamcat"), pattern=glob2rx(paste(csv_groups[j],"*.csv",sep="")), full.names=FALSE)
  for (i in 1:length(csv_files1)){  
    assign(paste("StreamCat",gsub("[.]csv$","",csv_files1[i]),sep="_") ,read.csv(here::here("data/streamcat",csv_files1[i]), header=TRUE,     stringsAsFactors=FALSE)[ ,-2:-5]) # the subset at the end skips the watershed and catchment area fields
  }
  assign(paste("StrCatCombined",csv_groups[j],sep="_"), do.call(rbind,  mget(ls(pattern = "StreamCat_*")) ) )
  
  
  print(paste(paste("StrCatCombined",csv_groups[j],sep="_"),"completed"))
  rm(list=ls(pattern="StreamCat_*"))
} 


# combine the landcover types into groups
StrCatCombined_NLCD2011_COMBINED <- StrCatCombined_NLCD2011_

### calculated urbanized
StrCatCombined_NLCD2011_COMBINED$PctUrbanized_2011Cat <- StrCatCombined_NLCD2011_COMBINED$PctUrbOp2011Cat + StrCatCombined_NLCD2011_COMBINED$PctUrbLo2011Cat + StrCatCombined_NLCD2011_COMBINED$PctUrbMd2011Cat + StrCatCombined_NLCD2011_COMBINED$PctUrbHi2011Cat # catchment level
StrCatCombined_NLCD2011_COMBINED$PctUrbOp2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbLo2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbMd2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbHi2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbanized_2011Ws <- StrCatCombined_NLCD2011_COMBINED$PctUrbOp2011Ws + StrCatCombined_NLCD2011_COMBINED$PctUrbLo2011Ws + StrCatCombined_NLCD2011_COMBINED$PctUrbMd2011Ws + StrCatCombined_NLCD2011_COMBINED$PctUrbHi2011Ws # watershed level
StrCatCombined_NLCD2011_COMBINED$PctUrbOp2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbLo2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbMd2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctUrbHi2011Ws <- NULL

### calulate ag
StrCatCombined_NLCD2011_COMBINED$PctAgricultural_2011Cat <- StrCatCombined_NLCD2011_COMBINED$PctHay2011Cat + StrCatCombined_NLCD2011_COMBINED$PctCrop2011Cat # catchment level
StrCatCombined_NLCD2011_COMBINED$PctHay2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctCrop2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctAgricultural_2011Ws <- StrCatCombined_NLCD2011_COMBINED$PctHay2011Ws + StrCatCombined_NLCD2011_COMBINED$PctCrop2011Ws # watershedlevel
StrCatCombined_NLCD2011_COMBINED$PctHay2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctCrop2011Ws <- NULL

### calculate natural cover
StrCatCombined_NLCD2011_COMBINED$PctNatCover_2011Cat <- StrCatCombined_NLCD2011_COMBINED$PctDecid2011Cat + StrCatCombined_NLCD2011_COMBINED$PctConif2011Cat + StrCatCombined_NLCD2011_COMBINED$PctMxFst2011Cat + StrCatCombined_NLCD2011_COMBINED$PctShrb2011Cat + StrCatCombined_NLCD2011_COMBINED$PctGrs2011Cat + StrCatCombined_NLCD2011_COMBINED$PctWdWet2011Cat + StrCatCombined_NLCD2011_COMBINED$PctHbWet2011Cat    # catachment level
StrCatCombined_NLCD2011_COMBINED$PctDecid2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctConif2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctMxFst2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctShrb2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctGrs2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctWdWet2011Cat <- NULL
StrCatCombined_NLCD2011_COMBINED$PctHbWet2011Cat<- NULL
StrCatCombined_NLCD2011_COMBINED$PctNatCover_2011Ws <- StrCatCombined_NLCD2011_COMBINED$PctDecid2011Ws + StrCatCombined_NLCD2011_COMBINED$PctConif2011Ws + StrCatCombined_NLCD2011_COMBINED$PctMxFst2011Ws + StrCatCombined_NLCD2011_COMBINED$PctShrb2011Ws + StrCatCombined_NLCD2011_COMBINED$PctGrs2011Ws + StrCatCombined_NLCD2011_COMBINED$PctWdWet2011Ws + StrCatCombined_NLCD2011_COMBINED$PctHbWet2011Ws    # watershed level
StrCatCombined_NLCD2011_COMBINED$PctDecid2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctConif2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctMxFst2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctShrb2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctGrs2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctWdWet2011Ws <- NULL
StrCatCombined_NLCD2011_COMBINED$PctHbWet2011Ws< - NULL

# rearrange columns
StrCatCombined_NLCD2011_COMBINED <- StrCatCombined_NLCD2011_COMBINED[c("COMID","PctOw2011Cat","PctIce2011Cat","PctBl2011Cat","PctUrbanized_2011Cat","PctAgricultural_2011Cat","PctNatCover_2011Cat","PctOw2011Ws","PctIce2011Ws","PctBl2011Ws","PctUrbanized_2011Ws","PctAgricultural_2011Ws","PctNatCover_2011Ws")]



# Reference Stream Temp Predicted
### need to figure out what to do.
# StrCatCombined_RefStreamTempPred_$MSST <- apply(StrCatCombined_RefStreamTempPred_[2:5], 1, mean)


# The following tables are not editted or modified and should be left as-is:
## 1)  StrCatCombined_Elevation_
## 2)  StrCatCombined_Kffact_
## 3)  StrCatCombined_NABD_
## 4)  StrCatCombined_WetIndx_
## 5)  StrCatCombined_STATSGO_Set1_
## 6)  StrCatCombined_STATSGO_Set2_
## 7)  StrCatCombined_Runoff_
## 8)  StrCatCombined_RoadStreamCrossings_
## 9)  StrCatCombined_RoadDensity_
## 10) StrCatCombined_RoadDensityRipBuf100_
## 11) StrCatCombined_PRISM_1981_2010_
## 12) StrCatCombined_NADP_
## 13) StrCatCombined_Mines_
## 14) StrCatCombined_MinesRipBuf100_
## 15) StrCatCombined_CoalMines_
## 16) StrCatCombined_AgMidHiSlopes_
## 17) StrCatCombined_AgriculturalNitrogen_  # is this correlated with landcover?
## 18) StrCatCombined_BFI_
## 19) StrCatCombined_CanalDensity_
## 20) StrCatCombined_Dams_
## 21) StrCatCombined_GeoChemPhys1_
## 22) StrCatCombined_GeoChemPhys2_
## 23) StrCatCombined_GeoChemPhys3_
## 24) StrCatCombined_GeoChemPhys4_
## 25) StrCatCombined_ICI_IWI_v1_
## 26) StrCatCombined_ImperviousSurfaces2011_
## 27) StrCatCombined_ImperviousSurfaces2011RipBuf100_
## 28) StrCatCombined_Lithology_

# combine the geochemical and physical tables
StrCatCombined_GeoChemPhysCOMBINED <- merge(StrCatCombined_GeoChemPhys1_,StrCatCombined_GeoChemPhys2_,by="COMID")
StrCatCombined_GeoChemPhysCOMBINED <- merge(StrCatCombined_GeoChemPhysCOMBINED,StrCatCombined_GeoChemPhys3_,by="COMID")
StrCatCombined_GeoChemPhysCOMBINED <- merge(StrCatCombined_GeoChemPhysCOMBINED,StrCatCombined_GeoChemPhys4_,by="COMID")

# combine the soil tables
StrCatCombined_STATSGO_COMBINED <- merge(StrCatCombined_STATSGO_Set1_,StrCatCombined_STATSGO_Set2_,by="COMID")

# combine road data
StrCatCombined_RoadStreamCOMBINED <- merge(StrCatCombined_RoadStreamCrossings_,StrCatCombined_RoadDensity_,by="COMID")
StrCatCombined_RoadStreamCOMBINED <- merge(StrCatCombined_RoadStreamCOMBINED,StrCatCombined_RoadDensityRipBuf100_,by="COMID")

# combine impervious surface data
StrCatCombined_ImperviousSurfaces2011_MERGE <- merge(StrCatCombined_ImperviousSurfaces2011_,StrCatCombined_ImperviousSurfaces2011RipBuf100_,by="COMID")




### Start to build a giant dataset
MoBI_FlowEnvVar <- merge(NHD_MoBI1, StrCatCombined_GeoChemPhysCOMBINED, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_STATSGO_COMBINED, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_RoadStreamCOMBINED, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_ImperviousSurfaces2011_MERGE, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_Elevation_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_Kffact_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_NABD_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_WetIndx_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_Runoff_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_PRISM_1981_2010_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_NADP_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_AgMidHiSlopes_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_AgriculturalNitrogen_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_BFI_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_CanalDensity_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_Dams_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_ICI_IWI_v1_, by="COMID",all.x=TRUE)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_Lithology_, by="COMID",all.x=TRUE)

##MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, , by="COMID",all.x=TRUE)


# delete the following tables
rm(StrCatCombined_USCensus2010_)
rm(StrCatCombined_USCensus2010RipBuf10_)
rm(StrCatCombined_NRSA_PredictedBioCondition_) # probability that a stream is in good biological condition
rm(StrCatCombined_NonAgIntrodManagVeg_)
rm(StrCatCombined_NonAgIntrodManagVegRipBuf100_)

# get the HUC12 data.  Built it through spatial joins in ArcPro.
huc12_network <- read.csv(here("spatial_data","huc12_network.csv"),stringsAsFactors=FALSE, colClasses=c("REACHCODE"="character","HUC_12"="character"))
huc12_nonnetwork <- read.csv(here("spatial_data","huc12_nonnetwork.csv"),stringsAsFactors=FALSE, colClasses=c("REACHCODE"="character","HUC_12"="character"))
huc12 <- rbind(huc12_network,huc12_nonnetwork)
rm(huc12_network,huc12_nonnetwork)
huc12$OBJECTID <- NULL
huc12 <- unique(huc12) # just for checks
# join to MoBI data
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, huc12[c("COMID","HUC_12")], by="COMID",all.x=TRUE)

# join the landcover data from above
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, StrCatCombined_NLCD2011_COMBINED, by="COMID",all.x=TRUE)

# sinousity
sin_net <- read.csv(here("data","sinuosity_net.csv"), stringsAsFactors=FALSE)
sin_net$OBJECTID <- NULL
sin_net$Enabled <- NULL
sin_nonnet <- read.csv(here("data","sinuosity_nonnet.csv"), stringsAsFactors=FALSE)
sin_nonnet$OBJECTID <- NULL
sin <- rbind(sin_net,sin_nonnet)
rm(sin_net, sin_nonnet)
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, sin, by="COMID",all.x=TRUE)

# bedrock depth
bedrockdepth <- read.csv(here("data","bedrockdepth_comid.csv"), stringsAsFactors=FALSE)
bedrockdepth <- bedrockdepth[c("COMID","MEAN")]
names(bedrockdepth)[2] <- "mnBedrockDepth"
MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar, bedrockdepth, by="COMID",all.x=TRUE)
rm(bedrockdepth)

# rearrange the whole table
MoBI_FlowEnvVar <- MoBI_FlowEnvVar[c("COMID","HUC_12","GNIS_ID","GNIS_NAME","LENGTHKM","REACHCODE","WBAREACOMI","FTYPE","FCODE","StreamLeve","StreamOrde","StreamCalc","SLOPE","Tidal","MAXELEVSMO","MINELEVSMO","ELEVFIXED","WBAreaType","V1_ComID","V1RchCode","V1Ftype","V2RchCode","V2Ftype","XWalkType","VC_MA","VA_MA","QC_MA","VC_MA","QE_MA","VE_MA","VC_01","VA_01","QC_01","VC_01","QE_01","VE_01","VC_02","VA_02","QC_02","VC_02","QE_02","VE_02","VC_03","VA_03","QC_03","VC_03","QE_03","VE_03","VC_04","VA_04","QC_04","VC_04","QE_04","VE_04","VC_05","VA_05","QC_05","VC_05","QE_05","VE_05","VC_06","VA_06","QC_06","VC_06","QE_06","VE_06","VC_07","VA_07","QC_07","VC_07","QE_07","VE_07","VC_08","VA_08","QC_08","VC_08","QE_08","VE_08","VC_09","VA_09","QC_09","VC_09","QE_09","VE_09","VC_10","VA_10","QC_10","VC_10","QE_10","VE_10","VC_11","VA_11","QC_11","VC_11","QE_11","VE_11","VC_12","VA_12","QC_12","VC_12","QE_12","VE_12","Al2O3Cat","CaOCat","Fe2O3Cat","K2OCat","MgOCat","Na2OCat","P2O5Cat","SCat","SiO2Cat","Al2O3Ws","CaOWs","Fe2O3Ws","K2OWs","MgOWs","Na2OWs","P2O5Ws","SWs","SiO2Ws","NCat","NWs","HydrlCondCat","HydrlCondWs","CompStrgthCat","CompStrgthWs","ClayCat","SandCat","ClayWs","SandWs","OmCat","PermCat","RckDepCat","WtDepCat","OmWs","PermWs","RckDepWs","WtDepWs","RdCrsCat","RdCrsSlpWtdCat","RdCrsWs","RdCrsSlpWtdWs","RdDensCat","RdDensWs","RdDensCatRp100","RdDensWsRp100","PctImp2011Cat","PctImp2011Ws","PctImp2011CatRp100","PctImp2011WsRp100","AgKffactCat","KffactCat","AgKffactWs","KffactWs","NABD_DensCat","NABD_NIDStorCat","NABD_NrmStorCat","NABD_DensWs","NABD_NIDStorWs","NABD_NrmStorWs","WetIndexCat","WetIndexWs","RunoffCat","RunoffWs","Precip8110Cat","Tmax8110Cat","Tmean8110Cat","Tmin8110Cat","Precip8110Ws","Tmax8110Ws","Tmean8110Ws","Tmin8110Ws","SN_2008Cat","InorgNWetDep_2008Cat","NH4_2008Cat","NO3_2008Cat","SN_2008Ws","InorgNWetDep_2008Ws","NH4_2008Ws","NO3_2008Ws","PctAg2006Slp20Cat","PctAg2006Slp10Cat","PctAg2006Slp20Ws","PctAg2006Slp10Ws","CBNFCat","FertCat","ManureCat","CBNFWs","FertWs","ManureWs","BFICat","BFIWs","CanalDensCat","CanalDensWs","DamDensCat","DamNIDStorCat","DamNrmStorCat","DamDensWs","DamNIDStorWs","DamNrmStorWs","CHYD","CCHEM","CSED","CCONN","CTEMP","CHABT","ICI","WHYD","WCHEM","WSED","WCONN","WTEMP","WHABT","IWI","PctCarbResidCat","PctNonCarbResidCat","PctAlkIntruVolCat","PctSilicicCat","PctExtruVolCat","PctColluvSedCat","PctGlacTilClayCat","PctGlacTilLoamCat","PctGlacTilCrsCat","PctGlacLakeCrsCat","PctGlacLakeFineCat","PctHydricCat","PctEolCrsCat","PctEolFineCat","PctSalLakeCat","PctAlluvCoastCat","PctCoastCrsCat","PctWaterCat","PctCarbResidWs","PctNonCarbResidWs","PctAlkIntruVolWs","PctSilicicWs","PctExtruVolWs","PctColluvSedWs","PctGlacTilClayWs","PctGlacTilLoamWs","PctGlacTilCrsWs","PctGlacLakeCrsWs","PctGlacLakeFineWs","PctHydricWs","PctEolCrsWs","PctEolFineWs","PctSalLakeWs","PctAlluvCoastWs","PctCoastCrsWs","PctWaterWs","PctOw2011Cat","PctIce2011Cat","PctBl2011Cat","PctUrbanized_2011Cat","PctAgricultural_2011Cat","PctNatCover_2011Cat","PctOw2011Ws","PctIce2011Ws","PctBl2011Ws","PctUrbanized_2011Ws","PctAgricultural_2011Ws","PctNatCover_2011Ws","sinuosity")] #,"mnBedrockDepth"




MoBI_FlowEnvVar <- merge(MoBI_FlowEnvVar,v1attributes,by.x="V1_ComID",by.y="COMID",all.x=TRUE)
MoBI_FlowEnvVar_Update <- MoBI_FlowEnvVar
MoBI_FlowEnvVar_Update$GNIS_ID <- NULL
MoBI_FlowEnvVar_Update$GNIS_NAME <- NULL
MoBI_FlowEnvVar_Update$LENGTHKM <- NULL
MoBI_FlowEnvVar_Update$REACHCODE <- NULL
MoBI_FlowEnvVar_Update$WBAREACOMI <- NULL
MoBI_FlowEnvVar_Update$FTYPE <- NULL
MoBI_FlowEnvVar_Update$FCODE <- NULL
MoBI_FlowEnvVar_Update$StreamLeve <- NULL
MoBI_FlowEnvVar_Update$StreamOrde <- NULL
MoBI_FlowEnvVar_Update$StreamCalc <- NULL
MoBI_FlowEnvVar_Update$WBAreaType <- NULL
MoBI_FlowEnvVar_Update$ELEVFIXED <- NULL
MoBI_FlowEnvVar_Update$V1_ComID <- NULL
MoBI_FlowEnvVar_Update$V1RchCode <- NULL
MoBI_FlowEnvVar_Update$V1Ftype <- NULL
MoBI_FlowEnvVar_Update$V2RchCode <- NULL
MoBI_FlowEnvVar_Update$V2Ftype <- NULL
MoBI_FlowEnvVar_Update$XWalkType <- NULL
MoBI_FlowEnvVar_Update$QA_01 <- NULL
MoBI_FlowEnvVar_Update$QA_02 <- NULL
MoBI_FlowEnvVar_Update$QA_03 <- NULL
MoBI_FlowEnvVar_Update$QA_04 <- NULL
MoBI_FlowEnvVar_Update$QA_05 <- NULL
MoBI_FlowEnvVar_Update$QA_06 <- NULL
MoBI_FlowEnvVar_Update$QA_07 <- NULL
MoBI_FlowEnvVar_Update$QA_08 <- NULL
MoBI_FlowEnvVar_Update$QA_09 <- NULL
MoBI_FlowEnvVar_Update$QA_10 <- NULL
MoBI_FlowEnvVar_Update$QA_11 <- NULL
MoBI_FlowEnvVar_Update$QA_12 <- NULL

MoBI_FlowEnvVar_Update$QC_01 <- NULL
MoBI_FlowEnvVar_Update$QC_02 <- NULL
MoBI_FlowEnvVar_Update$QC_03 <- NULL
MoBI_FlowEnvVar_Update$QC_04 <- NULL
MoBI_FlowEnvVar_Update$QC_05 <- NULL
MoBI_FlowEnvVar_Update$QC_06 <- NULL
MoBI_FlowEnvVar_Update$QC_07 <- NULL
MoBI_FlowEnvVar_Update$QC_08 <- NULL
MoBI_FlowEnvVar_Update$QC_09 <- NULL
MoBI_FlowEnvVar_Update$QC_10 <- NULL
MoBI_FlowEnvVar_Update$QC_11 <- NULL
MoBI_FlowEnvVar_Update$QC_12 <- NULL

MoBI_FlowEnvVar_Update$VC_01 <- NULL
MoBI_FlowEnvVar_Update$VC_02 <- NULL
MoBI_FlowEnvVar_Update$VC_03 <- NULL
MoBI_FlowEnvVar_Update$VC_04 <- NULL
MoBI_FlowEnvVar_Update$VC_05 <- NULL
MoBI_FlowEnvVar_Update$VC_06 <- NULL
MoBI_FlowEnvVar_Update$VC_07 <- NULL
MoBI_FlowEnvVar_Update$VC_08 <- NULL
MoBI_FlowEnvVar_Update$VC_09 <- NULL
MoBI_FlowEnvVar_Update$VC_10 <- NULL
MoBI_FlowEnvVar_Update$VC_11 <- NULL
MoBI_FlowEnvVar_Update$VC_12 <- NULL

MoBI_FlowEnvVar_Update$VE_01 <- NULL
MoBI_FlowEnvVar_Update$VE_02 <- NULL
MoBI_FlowEnvVar_Update$VE_03 <- NULL
MoBI_FlowEnvVar_Update$VE_04 <- NULL
MoBI_FlowEnvVar_Update$VE_05 <- NULL
MoBI_FlowEnvVar_Update$VE_06 <- NULL
MoBI_FlowEnvVar_Update$VE_07 <- NULL
MoBI_FlowEnvVar_Update$VE_08 <- NULL
MoBI_FlowEnvVar_Update$VE_09 <- NULL
MoBI_FlowEnvVar_Update$VE_10 <- NULL
MoBI_FlowEnvVar_Update$VE_11 <- NULL
MoBI_FlowEnvVar_Update$VE_12 <- NULL

MoBI_FlowEnvVar_Update$QE_01 <- NULL
MoBI_FlowEnvVar_Update$QE_02 <- NULL
MoBI_FlowEnvVar_Update$QE_03 <- NULL
MoBI_FlowEnvVar_Update$QE_04 <- NULL
MoBI_FlowEnvVar_Update$QE_05 <- NULL
MoBI_FlowEnvVar_Update$QE_06 <- NULL
MoBI_FlowEnvVar_Update$QE_07 <- NULL
MoBI_FlowEnvVar_Update$QE_08 <- NULL
MoBI_FlowEnvVar_Update$QE_09 <- NULL
MoBI_FlowEnvVar_Update$QE_10 <- NULL
MoBI_FlowEnvVar_Update$QE_11 <- NULL
MoBI_FlowEnvVar_Update$QE_12 <- NULL

MoBI_FlowEnvVar_Update$VA_01 <- NULL
MoBI_FlowEnvVar_Update$VA_02 <- NULL
MoBI_FlowEnvVar_Update$VA_03 <- NULL
MoBI_FlowEnvVar_Update$VA_04 <- NULL
MoBI_FlowEnvVar_Update$VA_05 <- NULL
MoBI_FlowEnvVar_Update$VA_06 <- NULL
MoBI_FlowEnvVar_Update$VA_07 <- NULL
MoBI_FlowEnvVar_Update$VA_08 <- NULL
MoBI_FlowEnvVar_Update$VA_09 <- NULL
MoBI_FlowEnvVar_Update$VA_10 <- NULL
MoBI_FlowEnvVar_Update$VA_11 <- NULL
MoBI_FlowEnvVar_Update$VA_12 <- NULL


# eastern region data
EastStream <- read.dbf(here("Eastern Stream Classification Tables","EasternStreamClass_Table.dbf"),as.is=FALSE)
EastStream <- EastStream[c("COMID","mean_diam","TempC","Stor1","DOR","DCI_up")]
# need to crosswalk v1 to V2
EastStream1 <- merge(EastStream, crosswalk[c("V1_ComID","V2_ComID")], by.x="COMID", by.y="V1_ComID")
EastStream1$COMID <- NULL
names(EastStream1)[6] <- "COMID"

MoBI_FlowEnvVar_Update <- merge(MoBI_FlowEnvVar_Update, EastStream1, by="COMID", all.x=TRUE)


# run correlation analysis
#correlationtree <- varclus(data.matrix(MoBI_FlowEnvVar_Update[3:173]), similarity="spear")
correlationtree <- varclus(data.matrix(MoBI_FlowEnvVar_Update[c(80:82,105:110,121,174:178)]), similarity="spear")
# plot the data
#pdf("file.pdf",width=24,height=12)
plot(correlationtree, cex=0.6)
abline(h=0.1, col="blue")
abline(h=0.2, col="red")
#dev.off()


# prevent duplicated pairs
var.corelation <- correlationtree$sim*lower.tri(correlationtree$sim)
check.corelation <- which(var.corelation>0.64, arr.ind=TRUE)
graph.cor <- graph.data.frame(check.corelation, directed = FALSE)
groups.cor <- split(unique(as.vector(check.corelation)),         clusters(graph.cor)$membership)
a <- lapply(groups.cor,FUN=function(list.cor){rownames(var.corelation)[list.cor]})
correlated_groups <- do.call("rbind", lapply(a, data.frame))
correlated_groups$group <- rownames(correlated_groups)
correlated_groups$group <- gsub("\\..*","",correlated_groups$group)  #use to join up with the lkupenvvars table
rm(a, var.corelation, check.corelation, groups.cor, graph.cor)

# sqlite loading
db_aqua_lotic <- dbConnect(SQLite(),"MoBI_Aquatic_EnvVars.sqlite")
dbWriteTable(db_aqua_lotic, "envvar_lotic", MoBI_FlowEnvVar_Update)
dbDisconnect(db_aqua_lotic)

# envvar descriptions
envvars <- colnames(MoBI_FlowEnvVar)
ev_desc <- as.data.frame(envvars)
for(i in 1:length(ev_desc$envvars)){
  ev_desc$mean <- mean(get(MoBI_FlowEnvVar)$ev_desc$envvars[i])
  #get(paste('r',i,collapse='',sep=''))$speed
}

ev_desc <- lapply(MoBI_FlowEnvVar[25:262], function(x) rbind( mean = mean(x, na.rm=TRUE) ,
                                         sd = sd(x, na.rm=TRUE) ,
                                         median = median(x, na.rm=TRUE) ,
                                         minimum = min(x, na.rm=TRUE) ,
                                         maximum = max(x, na.rm=TRUE) ,
                                         s.size = length(x) ) )











