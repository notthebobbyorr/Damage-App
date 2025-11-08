if (basename(getwd()) != "Damage-App") {
  setwd(file.path(getwd(), "Damage-App"))
}

print(here::here())

require(RPostgres)
require(here)
require(tidyverse)
require(data.table)
require(catboost)
require(xgboost)
require(mgcv)

# Ensure data directory exists
if (!dir.exists(here::here("data"))) {
  dir.create(here::here("data"), recursive = TRUE)
}


# loading models in first step to avoid complicated debug
#
damage_mod <- readRDS(here("models", 'damage_rate_model.rds'))
new.xwt <- xgb.load(here("models", 'updated_XWT'))
xgb.lwt <- readRDS(here("models",'XWT MODEL.rds'))
xgb.woba <- readRDS(here("models",'xgb_woba.rds'))
bam.xwt.gain <- readRDS(here("models",'decision_values_model.rds'))
aa <- readRDS(here("models", 'new_arm_angle_fmla.rds'))
zone.bip <- catboost.load_model(here("models", 'catboost_zone_bip'))
zone.damage <- catboost.load_model(here("models", 'zone_damage'))
zone.rv <- catboost.load_model(here("models", 'catboost_zone_rv'))
dmg.pitching <- catboost.load_model(here("models", 'dmg_pitching_catboost'))
bip.pitching <- catboost.load_model(here("models", 'bip_pitching_catboost'))
hr_xgb <- xgb.load(here("models", 'hr_model'))
rwt.2s.cat <- catboost.load_model(here("models", 'starter_cat_stuff_nov8'))
putaway.cat <- catboost.load_model(here("models", 'new_putaway_nov11'))
whiff.prob <- catboost.load_model(here("models", 'all_vars_whiff_prob_2str'))


# setting up SQL pull
user <- Sys.getenv("DB_USER")
pass <- Sys.getenv("DB_PASS")
host <- Sys.getenv("DB_HOST")
dbase <- Sys.getenv("DB_DATABASE")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  user = user,
  password = pass,
  host = host,
  dbname = dbase
)

# querying PI & MLBAPI for season data
query  <-"select season, game_pk, game_date, game_type, level_id,
inning, half_inning, home_mlbid, away_mlbid, umpire_home_mlbid,
at_bat_index, event_index, pitch_of_ab, 
is_in_play, is_strike, is_ball, strikes_before, balls_before,
batter_mlbid, batter_bpid, pitcher_mlbid, pitcher_bpid, batter_hand, batter_position
,pitcher_hand, batter_name_first, batter_name_last, 
pitcher_name_first, pitcher_name_last, pitcher_role
,runs_home_before, runs_away_before
,coalesce(px_corr,px_adj,px_orig) as x
,coalesce(pz_corr,pz_adj,pz_orig) as z
,coalesce(x_angle_plate_adj,x_angle_plate_corr) as haa_orig
,coalesce(z_angle_plate_adj,z_angle_plate_corr) as vaa
,coalesce(start_speed_55_corr,start_speed_55_adj,start_speed_orig) as pitch_velo
,coalesce(end_speed_orig, end_speed_adj, end_speed_corr) as end_velo
,coalesce(plate_time_orig) as plate_time
,coalesce(x_angle_release_adj  ,x_angle_release_corr) as x_angle_release
,coalesce(z_angle_release_adj  ,z_angle_release_corr) as z_angle_release
,coalesce(extension_orig,60.5-y_release_adj,60.5-y_release_corr) as ext
,coalesce(obs_spin_rate_orig, obs_spin_rate_corr) as rpm
,coalesce(pfx_x_corr,pfx_x_adj) as pfx_x_short
,coalesce(pfx_x_long_corr,pfx_x_long_adj) as pfx_x_long
,coalesce(pfx_z_corr,pfx_z_adj) as pfx_z_short
,coalesce(pfx_z_long_corr,pfx_z_long_adj) as pfx_z_long
,coalesce(x55_corr, x55_adj) as x55
,coalesce(z55_corr, z55_adj) as z55
,coalesce(x_release_corr, x_release_adj) as release_x
,coalesce(z_release_corr, z_release_adj) as release_z
,inf_spin_axis_corr, obs_spin_axis_orig
,swing_type, is_contact, outs_before
,on_1b_mlbid, on_2b_mlbid, on_3b_mlbid
,stolen_base, caught_stealing, passed_ball
,coalesce(cs_prob) as called_strike_prob
,pi_zone_top, pi_zone_bottom, description, pitch_outcome
,pitch_result, seconds_between
,is_inzone_pi, elevation, temperature_game, batter_height
,pitcher_height, 
park_mlbid,
vx0_orig as vx0,
vy0_orig as vy0,
vz0_orig as vz0,
x0_orig as x0,
y0_orig as y0,
  z0_orig as z0
,coalesce(ax_adj, ax_corr) as ax
,coalesce(az_adj, az_corr) as az
,coalesce(ay_adj, ay_corr)  as ay
,coalesce(obs_spin_axis_orig, obs_spin_axis_corr) as axis,
pi_pitch_type, pi_pitch_sub_type, pi_pitch_group
from pitchinfo.pitches_public
where season in (2025)
           and level_id in (1, 11, 14, 16)
           and game_type in ('R')
          "

ask <- dbSendQuery(con, query) 
nu <- dbFetch(ask) # grabbing data from PI

# now grab batted balls
# create list vector to pass into the query
ids <- paste(nu$game_pk, collapse = ",")

# make the query 
ask_mlb <- paste("select * 
from mlbapi.batted_balls where
game_pk in (",paste(ids, sep = ","),")")

# pull batted balls
bb <- dbGetQuery(con, ask_mlb)

# rename for future join
bb %>% 
  dplyr::rename(
    pitch_of_ab = pitch_number) -> bb

# make the query 
ask_mlb <- paste("select * from mlbapi.baseout where game_pk in (",paste(ids, sep = ","),")")

# pull outs
outs <- dbGetQuery(con, ask_mlb)

# join the pulls
nu %>% left_join(bb, by = c('at_bat_index', 'event_index', 'pitch_of_ab', 'game_pk')) -> nu

# join again
nu %>% left_join(outs, by = c('at_bat_index', 'event_index', #'pitch_of_ab',
                              'game_pk')) -> nu

# create spray, assign, names, linear weights, woba, slugging

nu %>% mutate(
  hc_x = coord_x - 125.42,
  hc_y = 198.27 - coord_y,
  spray_angle = atan(hc_x/hc_y)*(180/pi*.75),
  spray_angle_adj = case_when(
    batter_hand == 'L' ~ -1 * spray_angle,
    T ~ spray_angle
  ),
  hitter_name = paste0(batter_name_first," ",batter_name_last),
  pitcher_name = paste0(pitcher_name_first," ",pitcher_name_last),
  lwt = case_when(
    (pitch_outcome == "HBP") ~ .38,
    # in play
    (is_in_play == T & pitch_outcome == "1B") ~ .47,
    (is_in_play == T & pitch_outcome == "2B") ~ .78,
    (is_in_play == T & pitch_outcome == "3B") ~ 1.05,
    (is_in_play == T & pitch_outcome == "HR") ~ 1.38,
    (is_in_play == T & pitch_outcome == "OUT") ~ -.26, 
    # end of AB
    (strikes_before == 2 & pitch_outcome == 'S') ~ -.28,
    (balls_before == 3 & pitch_outcome == 'B') ~ .34,
    T ~ 0
  ),
  woba = case_when(
    pitch_outcome == 'B' & balls_before == 3 ~ .692,
    pitch_outcome == 'HBP'~ .722,
    pitch_outcome == '1B' ~ .879,
    pitch_outcome == '2B' ~ 1.242,
    pitch_outcome == '3B' ~ 1.568,
    pitch_outcome == 'HR' ~ 2.007,
    T ~ 0
  ),
  slg = case_when(
    pitch_outcome == '1B' ~ 1,
    pitch_outcome == '2B' ~ 2,
    pitch_outcome == '3B' ~ 3,
    pitch_outcome == 'HR' ~ 4,
    T ~ 0
  )
) -> nu

# set rough zone estimates
cent_height = 2.5
cent_wide = 0

nu %>%
  mutate(
    zone_center = (pi_zone_top + pi_zone_bottom)/2) -> nu

# rename to LS to EV
nu %>% dplyr::rename(
  exit_velo = launch_speed
) -> nu

# adding easily referencable date vars
nu %>% mutate(
  Date = as.Date(game_date, format = "%Y-%m-%d"),
  Season = format(Date, format = "%Y"),
  Month = format(Date, format = "%m")
) -> nu

# standardizing the -1 to 1 scale for batter handedness
nu %>% mutate(
  haa_adj = case_when(
    batter_hand == 'L' ~ -1 * haa_orig, # break away = negative. break in = positive
    T ~ haa_orig),
  release_x_adj = case_when(
    batter_hand == 'L' ~ -1 * release_x,
    T ~ release_x
  ),
  x_angle_release_adj = case_when(
    batter_hand == 'L' ~ -1 * x_angle_release,
    T ~ x_angle_release
  ),
  x_adj = case_when(
    batter_hand == 'L' ~ -1 * x,
    T ~ x
  ),
  z_adj = (z * 12)/batter_height,
  z_adj_top = (pi_zone_top * 12)/batter_height,
  z_adj_bott = (pi_zone_bottom * 12)/batter_height,
  z_adj_center = (z_adj_top + z_adj_bott)/2,
  z_center = ((pi_zone_top - pi_zone_bottom)/2) + pi_zone_bottom,
  pfx_x_short_adj = case_when(
    batter_hand == 'L' ~ -1 * pfx_x_short,
    T ~ pfx_x_short
  ),
  pfx_x_long_adj = case_when(
    batter_hand == 'L' ~ -1 * pfx_x_long,
    T ~ pfx_x_long
  ),
  platoon_adv = case_when(
    batter_hand == pitcher_hand ~ 0,
    T ~ 1
  ),
  euc = sqrt((z - z_center)^2 + (x_adj - cent_wide)^2),
  league_euc = sqrt((z - zone_center)^2 + (x_adj - cent_wide)^2),
  euc_adj = sqrt((z_adj - z_adj_center)^2 + (x_adj - cent_wide)^2),
  count = factor(paste0(balls_before,"-",strikes_before)),
) %>% 
  mutate(
    park_mlbid = as.factor(park_mlbid)
  ) %>%
  group_by(batter_mlbid) %>%
  mutate(
    EV80 = quantile(exit_velo[is_in_play == T & exit_velo > 0], probs = .8, na.rm = T),
    EV80.x = mean(x[is_in_play == T & exit_velo > EV80], na.rm = T),
    EV80.x.adj = mean(x_adj[is_in_play == T & exit_velo > EV80], na.rm = T),
    EV80.z = mean(z[is_in_play == T & exit_velo > EV80], na.rm = T),
    EV80.z.adj = mean(z_adj[is_in_play == T & exit_velo > EV80], na.rm = T),
    euclid_80 = sqrt((z - EV80.z)^2 + (x - EV80.x)^2),
    euc_80_adj = sqrt((z_adj - EV80.z.adj)^2 + (x_adj - EV80.x.adj)^2)
  ) %>% 
  ungroup() -> nu

# mlb attack zones via tango -
# heart - .56 wide, .33 from pi_zone_top and pi_zone_bottom
# zone - .835  wide, pi_top & pi_bottom
# shadow - 1.11 wide, .33 + top & bottom - .33
# chase - 1.67 wide. 0 - 4.5 height

nu %>%
  mutate(
    zone_center = (pi_zone_top + pi_zone_bottom)/2,
    attack_zone = case_when(
      x_adj >= -.56 & x_adj <= .56 & z >= (pi_zone_bottom + .33) & z <= (pi_zone_top - .33) ~ 'heart',
      x_adj >= -.84 & x_adj <= .84 & z >= pi_zone_bottom & z <= pi_zone_top ~ 'zone',
      x_adj >= -1.11 & x_adj <= 1.11 & z >= (pi_zone_bottom - .33) & z <= (pi_zone_top + .33) ~ 'shadow',
      x_adj >= -1.67 & x_adj <= 1.67 & z >= 0 & z <= 4.5 ~ 'chase',
      T ~ 'waste'
    )
  ) -> nu

# exp linear weights
xwt.feats <- c('exit_velo', 'launch_angle', 'spray_angle_adj')

nu$xwt <- predict(new.xwt, newdata = nu %>% 
                    dplyr::select(all_of(xwt.feats)) %>% 
                    as.matrix(),
                  type = 'response')

# set min/max
nu %>%
  mutate(xwt = case_when(is_in_play == T & xwt < -.302 ~ -.302,
                         xwt > 1.29 ~ 1.29,
                         T ~ xwt)) -> nu


# my exp woba metric
# input
nu$xgb.woba <- as.numeric(predict(xgb.woba, newdata = nu %>% 
                                    dplyr::select(xgb.woba$feature_names) %>% 
                                    as.matrix(),
                                  type = 'response'))

# round it to correct limits
nu %>%
  mutate(
    xgb.woba = case_when(
      xgb.woba < 0 ~ 0,
      xgb.woba > 2.007 ~ 2.007,
      T ~ xgb.woba
    )) -> nu

####
# damage
# create rounded spray with correct feature name
nu$adjusted_spray <- round(nu$spray_angle_adj, 0)

# predict
nu$damage_pred <- as.numeric(predict(damage_mod, nu, type = 'response'))

# create binary dmg indicator variable (and whiff/swing)
nu %>% 
  dplyr::mutate(
    damage = case_when(
      exit_velo >= damage_pred & launch_angle > 0 & spray_angle_adj >= -50 & spray_angle_adj <= 50 ~ 1,
      T ~ 0
    ),
    whiff = case_when(
      is_contact == F & swing_type == 'swing' ~ 1,
      T ~ 0
    ),
    swing = case_when(
      swing_type == 'swing' ~ 1,
      T ~ 0
    )
  ) -> nu

# team codes
team_ids <- fread(here("data_static", 'team_ids.csv'))

# joining to create team filters
nu %>% mutate(
  hitting_id = case_when(
    half_inning == 'bottom' ~ home_mlbid,
    T ~ away_mlbid
  ),
  pitching_id = case_when(
    half_inning == 'bottom' ~ away_mlbid,
    T ~ home_mlbid
  )
) -> nu

nu %>% 
  left_join(team_ids %>% 
              filter(active == T) %>% 
              dplyr::select(id, name, team_name, abbreviation, venue), 
            by = c('hitting_id' = 'id')) %>% 
  dplyr::rename(
    hitting_team = name,
    hitting_team_name = team_name,
    hitting_code = abbreviation
  ) -> nu

nu %>% 
  left_join(team_ids %>% 
              filter(active == T) %>%
              dplyr::select(id, name, team_name, abbreviation),
            by = c('pitching_id' = 'id')) %>% dplyr::rename(
              pitching_team = name,
              pitching_team_name = team_name,
              pitching_code = abbreviation
            ) -> nu

### adding spin eff and ssw for models
##### Spin efficiency estimate
K <-  5.383E-03
z_constant <- 32.174

nu <- nu %>% 
  mutate(yR = 60.5 - ext) %>% 
  mutate(tR =(-vy0-sqrt(vy0^2-2*ay*(50-yR)))/ay) %>% 
  mutate(vxR = vx0+ax*tR, 
         vyR = vy0+ay*tR,
         vzR = vz0+az*tR
  ) %>% 
  mutate(tf = (-vyR-sqrt(vyR^2-2*ay*(yR-17/12)))/ay) %>% 
  mutate(vxbar = (2*vxR+ax*tf)/2, 
         vybar = (2*vyR+ay*tf)/2,
         vzbar = (2*vzR+az*tf)/2 
  ) %>% 
  mutate(vbar = sqrt(vxbar^2+vybar^2+vzbar^2)) %>% 
  mutate(adrag = -(ax*vxbar+ay*vybar+(az+z_constant)*vzbar)/vbar) %>% 
  mutate(amagx = ax+adrag*vxbar/vbar, 
         amagy = ay+adrag*vybar/vbar,
         amagz = az+adrag*vzbar/vbar+z_constant 
  ) %>%  
  mutate(amag = sqrt(amagx^2 + amagy^2 + amagz^2)) %>% 
  mutate(Cl = amag/(K*vbar^2)) %>% 
  mutate(s = 0.166*log(0.336/(0.336-Cl))) %>% 
  mutate(spinT = 78.92*s*vbar) %>% 
  mutate(spin_efficiency = .1 + (spinT/rpm)) %>%
  mutate(spin_efficiency = case_when(
    spin_efficiency > 1 ~ 1,
    T ~ spin_efficiency
  ))

# savant arm angles
nu$ball_angle <- predict(aa, newdata = nu, type = 'response')


# ssw
nu <- nu %>% 
  mutate(
    ssw_delta = obs_spin_axis_orig - inf_spin_axis_corr,
    ivb = 1.8 * pfx_z_short,
    hb = 1.8 * pfx_x_short_adj,
    hb_orig = 1.8 * pfx_x_short,
    platoon = as.factor(paste0(batter_hand,"_",pitcher_hand)),
    platoon_matchup = as.factor(ifelse(pitcher_hand == batter_hand, 'same', 'opp')))

# separate into new pitch tags
nu %>%
  mutate(
    pitch_tag = case_when(
      pi_pitch_sub_type == 'SW' ~ 'SW', # sweepers are their own thing
      pi_pitch_type == 'SI' ~ 'SI', # sinkers separate from fastballs
      pi_pitch_group == 'FA' & pi_pitch_type == 'FC' ~ 'HC', # hard cutters their own designation
      pi_pitch_type == 'FS' ~ 'FS', # separate splits from changes
      pi_pitch_type == 'FA' ~ 'FA',
      pi_pitch_group == 'SL' ~ 'SL',
      pi_pitch_type == 'CH' ~ 'CH',
      pi_pitch_group == 'CU' ~ 'CU',
      T ~ 'XX'
    )
  ) -> nu

with(nu, table(pi_pitch_type, pitch_tag)) # check crossover

# the other variables - usage is one
nu %>%
  #drop_na(pitcher_mlbid) %>%
  group_by(pitcher_mlbid, level_id, season) %>% 
  mutate(pitches = n()) %>%
  group_by(pitcher_mlbid, level_id, season, pitch_tag) %>% 
  mutate(type_pitches = n(),
         pct = type_pitches/pitches) %>%
  ungroup() -> nu

# finding primary hard pitch
nu %>%
  filter(pitch_tag %in% c('FA', 'SI', 'HC')) %>%
  group_by(pitcher_mlbid, pitcher_name, level_id, season, pitch_tag) %>%
  summarise(total_pct = sum(pct)) %>%
  group_by(pitcher_mlbid, level_id, season) %>%
  filter(total_pct == max(total_pct)) %>%
  mutate(main = pitch_tag) -> mains

# joining to create main pitch column
mains %>% 
  ungroup() %>%
  dplyr::select(pitcher_mlbid, level_id, season, main) %>%
  left_join(nu, 
            by = c('pitcher_mlbid', 'level_id', 'season'),
            relationship = "many-to-many") -> nu

# creating differentials from pitches for stuff models
nu %>%
  ungroup() %>%
  #drop_na(pitcher_mlbid) %>%
  mutate(
    height_release_delta = (pitcher_height/12) - release_z,
    x_angle_release_adj = case_when(
      batter_hand == 'L' ~ -1 * x_angle_release,
      T ~ x_angle_release)
  ) %>%
  group_by(pitcher_mlbid, level_id, season) %>% 
  mutate(
    main_vaa = mean(vaa[pitch_tag == main], na.rm = T),
    main_rpm = mean(rpm[pitch_tag == main], na.rm = T),
    main_haa = mean(haa_adj[pitch_tag == main], na.rm = T),
    main_haa_orig = mean(haa_orig[pitch_tag == main], na.rm = T),
    main_z_angle = mean(z_angle_release[pitch_tag == main], na.rm = T),
    main_x_angle = mean(x_angle_release[pitch_tag == main], na.rm = T),
    main_x_angle_adj = mean(x_angle_release_adj[pitch_tag == main], na.rm = T),
    main_velo = mean(pitch_velo[pitch_tag == main], na.rm = T),
    main_ivb = mean(ivb[pitch_tag == main], na.rm = T),
    main_hb = mean(hb[pitch_tag == main], na.rm = T),
    main_ball_angle = mean(ball_angle[pitch_tag == main], na.rm = T),
    main_hb_orig = mean(hb_orig[pitch_tag == main], na.rm = T),
    main_axis = mean(axis[pitch_tag == main], na.rm = T),
    main_rpm = mean(rpm[pitch_tag == main], na.rm = T)) %>%
  ungroup() %>%
  group_by(pitcher_mlbid, level_id, season, pitch_tag) %>%
  mutate(
    vaa_delta = ifelse(pitch_tag != main, vaa - main_vaa, 0),
    haa_delta = ifelse(pitch_tag != main, haa_adj - main_haa, 0),
    haa_orig_delta = ifelse(pitch_tag != main, haa_orig - main_haa_orig, 0),
    ball_angle_delta = ifelse(pitch_tag != main, ball_angle - main_ball_angle, 0),
    z_angle_delta = ifelse(pitch_tag != main, z_angle_release - main_z_angle, 0),
    x_angle_delta = ifelse(pitch_tag != main, x_angle_release - main_x_angle, 0),
    x_angle_adj_delta = ifelse(pitch_tag != main, x_angle_release_adj - main_x_angle_adj, 0),
    velo_delta = ifelse(pitch_tag != main, pitch_velo - main_velo, 0),
    ivb_delta = ifelse(pitch_tag != main, ivb - main_ivb, 0),
    hb_delta = ifelse(pitch_tag != main, hb - main_hb, 0),
    hb_orig_delta = ifelse(pitch_tag != main, hb_orig - main_hb_orig, 0),
    axis_delta = ifelse(pitch_tag != main, axis - main_axis, 0),
    rpm_delta = ifelse(pitch_tag != main, rpm - main_rpm, 0)
  ) %>% 
  ungroup() -> nu

# a few more vars I used occasionally
nu %>%
  mutate(
    x_if_main = x_adj + (hb_delta/12),
    z_if_main = z + (ivb_delta/12),
    strike_if_main = case_when(
      x_if_main >= -.835 & x_if_main <= .835 & z_if_main > pi_zone_bottom & z_if_main < pi_zone_top ~ 1,
      T ~ 0
    ),
    pitch_group = as.factor(case_when(
      pi_pitch_group == 'FA' ~ 'FA',
      pi_pitch_group %in% c('CU', 'SL') ~ 'BR',
      pi_pitch_group %in% c('KN', 'CH') ~ 'CH',
      T ~ pi_pitch_group
    ))
  ) -> nu

# bins I've used on occasion for the strike zone
x_adj_min <- -1.7
x_adj_max <- 1.7
z_min <- 0
z_max <- 4.5

# Create the grid
x_adj_breaks <- seq(x_adj_min, x_adj_max, length.out = 11)
z_breaks <- seq(z_min, z_max, length.out = 11)

# Bin the coordinates
nu$bin_x <- cut(nu$x_adj, breaks = x_adj_breaks, labels = FALSE)
nu$bin_z <- cut(nu$z, breaks = z_breaks, labels = FALSE)

# adding player positions - comes in handy for damage app
nu %>%
  mutate(
    batter_position = factor(batter_position),
    position = case_when(
      batter_position == 1 ~ 'P',
      batter_position == 2 ~ 'C',
      batter_position == 3 ~ 'X1B',
      batter_position == 4 ~ 'X2B',
      batter_position == 5 ~ 'X3B',
      batter_position == 6 ~ 'SS',
      batter_position %in% c(7, 8, 9) ~ 'OF',
      batter_position %in% c(10, 11, 12) ~ 'UT',
      T ~ 'NA'
    ),
    contact = ifelse(is_contact == T, 1, 0)
  ) -> nu


# setting count to a factor for prediction
nu$count <- as.factor(nu$count)

# this is the step for loading a pre-trained model - SEAGER
# inputting the swing-take values with trained/loaded model
nu$swing.decision.xwt <- predict(bam.xwt.gain, 
                                 newdata = nu,
                                 type = 'response')

# input values are assuming a swing, so we need to correct for a take:
nu %>%
  mutate(
    decision.value = case_when(swing == 1 ~ swing.decision.xwt,
                               swing == 0 ~ -1 * swing.decision.xwt)
  ) -> nu

# create unique PA id
nu %>%
  mutate(PA_id = paste(game_pk,"-", at_bat_index)) %>%
  arrange(PA_id, pitch_of_ab) %>%
  group_by(PA_id) %>%
  mutate(terminal_pitch = ifelse(row_number() == n(), 1, 0)) %>%
  ungroup() -> nu

# adding state changes
nu %>%
  mutate(strikes_after = case_when(pitch_outcome == 'S' ~ strikes_before + 1,
                                   T ~ strikes_before),
         balls_after = case_when(pitch_outcome == 'B' ~ balls_before + 1,
                                 T ~ balls_before),
         count_after = as.factor(paste0(balls_after,"-",strikes_after))) -> nu


# adding in run values
nu %>%
  mutate(runs_start = home_score_start + away_score_start,
         runs_end = home_score_end + away_score_end,
         runs_scored = runs_end - runs_start,
         half_inn_id = paste0(game_pk, inning, hitting_code)) -> nu

# create baserunner/out game stat variable. start with baserunner dummy var
nu %>%
  mutate(on_1b = ifelse(!is.na(on_1b_mlbid), 1, 0),
         on_2b = ifelse(!is.na(on_2b_mlbid), 1, 0),
         on_3b = ifelse(!is.na(on_3b_mlbid), 1, 0),
         base_state = paste0(on_1b, on_2b, on_3b),
         state = paste0(on_1b, on_2b, on_3b," ", outs_start),
         outs_recorded = outs_end - outs_start
  ) -> nu

# adding in stuff and whiff probs for the pitchers
# set up catboost pool with required features for models
target <- 'bip'

named_features_second <- 
  as.character(c(target, expression(pitcher_hand, 
                                    batter_hand,
                                    height_release_delta,
                                    x, z,
                                    #vaa_vs_avg, haa_vs_avg,
                                    pitch_velo, ivb, hb_orig,
                                    vaa,
                                    vaa_delta,
                                    haa_orig, 
                                    haa_orig_delta,
                                    ivb_delta, hb_orig_delta, 
                                    velo_delta, 
                                    z_angle_delta, x_angle_delta, 
                                    rpm, spin_efficiency, ssw_delta,
                                    axis, release_z, release_x, ext, 
                                    z_angle_release, x_angle_release,
                                    obs_spin_axis_orig, inf_spin_axis_corr
                                    
                                    
  )))

feat <- named_features_second

stuff_pool_second <- catboost.load_pool(nu %>% 
                                          mutate(x_rd = round(x_adj, 0),
                                                 z_rd = round(z, 0),
                                                 count = as.factor(count),
                                                 is.main = as.factor(case_when(pitch_tag == main ~ 1,
                                                                               T ~ 0)),
                                                 pitch_tag = as.factor(pitch_tag),
                                                 strikes_before = as.factor(strikes_before),
                                                 platoon_status = as.factor(case_when(batter_hand == pitcher_hand ~ 'same',
                                                                                      T ~ 'diff')),
                                                 strikes_before = factor(strikes_before),
                                                 pitcher_hand = factor(pitcher_hand),
                                                 batter_hand = factor(batter_hand),
                                                 platoon = as.factor(platoon)) %>%
                                          dplyr::select(all_of(feat[-1])))


# bip prob based on traits
nu$zone.bip <- catboost.predict(model = zone.bip,
                                pool = stuff_pool_second,
                                prediction_type = "Probability") # prob of swing
# flipping bip prob
nu$zone.nip <- 1 - nu$zone.bip

# damage prob based on traits
nu$zone.damage <- catboost.predict(model = zone.damage,
                                   pool = stuff_pool_second,
                                   prediction_type = "Probability")

# run values based on traits
nu$zone.rv <- .1 - catboost.predict(model = zone.rv, # have to *10 for overall quality
                                    pool = stuff_pool_second)

# applying nip prob to run value
nu$zone.nrv <- nu$zone.nip * nu$zone.rv

## pitching plus style damage calcs
nu$dmg.pitching <- catboost.predict(dmg.pitching, 
                                    stuff_pool_second,
                                    prediction_type = "Probability")

nu$bip.pitching <- catboost.predict(bip.pitching,
                                    stuff_pool_second,
                                    prediction_type = "Probability")

nu$dmg.prob <- with(nu, dmg.pitching * bip.pitching)

nu$xgb.hr <- predict(hr_xgb, newdata = nu %>% 
                        ungroup() %>%
                        dplyr::select(exit_velo, launch_angle, spray_angle) %>%
                        as.matrix(), type = 'response')

nu$xgb.hr <- ifelse(nu$xgb.hr < 0, 0, nu$xgb.hr)

# newer stuff models
target <- 'neut_euc_rwt_2s'

new_stuff_feats <-
  as.character(c(target, expression(
    batter_hand,
    platoon_matchup,
    height_release_delta,
    ball_angle, ball_angle_delta, 
    release_z, release_x, ext,
    pitch_velo, velo_delta,
    ivb, ivb_delta,
    hb_orig, hb_orig_delta,
    vaa, vaa_delta,
    haa_orig, haa_orig_delta,
    axis, axis_delta,
    rpm, rpm_delta, 
    spin_efficiency, ssw_delta,
    inf_spin_axis_corr,
    z_angle_release, z_angle_delta,
    x_angle_release, x_angle_delta
    
  )))

feat <- new_stuff_feats

stuff_pool_second <- catboost.load_pool(nu %>% 
                                          mutate(x_rd = round(x_adj, 0),
                                                 z_rd = round(z, 0),
                                                 count = as.factor(count),
                                                 is.main = as.factor(case_when(pitch_tag == main ~ 1,
                                                                               T ~ 0)),
                                                 pitch_tag = as.factor(pitch_tag),
                                                 strikes_before = as.factor(strikes_before),
                                                 platoon_status = as.factor(case_when(batter_hand == pitcher_hand ~ 'same',
                                                                                      T ~ 'diff')),
                                                 strikes_before = factor(strikes_before),
                                                 pitcher_hand = factor(pitcher_hand),
                                                 batter_hand = factor(batter_hand),
                                                 platoon = as.factor(platoon),
                                                 platoon_matchup = as.factor(ifelse(batter_hand == pitcher_hand, 'same', 'opp'))) %>%
                                          dplyr::select(all_of(feat[-1])))

nu$put.cat <- catboost.predict(putaway.cat, stuff_pool_second #, prediction_type = "Probability"
)

nu$rwt.2s.cat <- catboost.predict(rwt.2s.cat, stuff_pool_second #, prediction_type = "Probability"
)

all_stuff_feats <-
  as.character(c(target, expression(
    batter_hand,
    pitcher_hand,
    platoon_matchup,
    count,
    x, z, euc,
    height_release_delta,
    ball_angle, ball_angle_delta, 
    release_z, release_x, ext,
    pitch_velo, velo_delta,
    ivb, ivb_delta,
    hb_orig, hb_orig_delta,
    vaa, vaa_delta,
    haa_orig, haa_orig_delta,
    axis, axis_delta,
    rpm, rpm_delta, 
    spin_efficiency, ssw_delta,
    inf_spin_axis_corr,
    z_angle_release, z_angle_delta,
    x_angle_release, x_angle_delta
    
  )))

feat <- all_stuff_feats

stuff_pool_second <- catboost.load_pool(nu %>% 
                                          mutate(x_rd = round(x_adj, 0),
                                                 z_rd = round(z, 0),
                                                 count = as.factor(count),
                                                 is.main = as.factor(case_when(pitch_tag == main ~ 1,
                                                                               T ~ 0)),
                                                 pitch_tag = as.factor(pitch_tag),
                                                 strikes_before = as.factor(strikes_before),
                                                 platoon_status = as.factor(case_when(batter_hand == pitcher_hand ~ 'same',
                                                                                      T ~ 'diff')),
                                                 strikes_before = factor(strikes_before),
                                                 pitcher_hand = factor(pitcher_hand),
                                                 batter_hand = factor(batter_hand),
                                                 platoon = as.factor(platoon),
                                                 platoon_matchup = as.factor(ifelse(batter_hand == pitcher_hand, 'same', 'opp'))) %>%
                                          dplyr::select(all_of(feat[-1])))


# whiff prob based on all loc + traits
nu$whiff.prob <- catboost.predict(model = whiff.prob,
                                  pool = stuff_pool_second,
                                  prediction_type = 'Probability')

nu$exp.contact <- with(nu, 1 - whiff.prob)
nu$over.exp.contact <- with(nu, contact - exp.contact)

# base change variable
nu %>%
  group_by(game_pk) %>%
  arrange(at_bat_index, pitch_of_ab) %>%
  mutate(base_change = ifelse(base_state != lag(base_state), TRUE, FALSE)) %>%
  ungroup() -> nu


nu %>%
  mutate(wt = case_when(pitch_outcome == 'HR' ~ 1.29,
                        pitch_outcome == '3B' ~ .877,
                        pitch_outcome == '2B' ~ .642,
                        pitch_outcome == '1B' ~ .412,
                        pitch_outcome == 'RBOE' ~ .343,
                        pitch_outcome == 'HBP' ~ .352,
                        pitch_outcome == 'B' & balls_before == 3 ~ .347,
                        description == 'Ball In Dirt' ~ .214,
                        pitch_outcome == 'S' & strikes_before == 2 ~ -.309,
                        #pitch_outcome == 'OUT' & base_change == T ~ -.125,
                        #is_in_play == T & outs_recorded == 2 ~ -.862,
                        is_in_play == T & pitch_outcome == 'OUT' ~ -.302,
                        T ~ 0),
         wt_per_pitch = case_when(whiff == 1 ~ -.113,
                                  pitch_outcome == 'S' & swing == 0 ~ -.0257,
                                  pitch_outcome == 'B' ~ .0329,
                                  pitch_outcome == 'F' ~ 0,
                                  is_in_play == T ~ xwt,
                                  T ~ wt),
         wt_ao = wt - -.302) -> nu

nu %>%
  mutate(xwt_ao = xwt + .302,
         xwt_oba = case_when(
           pitch_outcome == 'B' & balls_before == 3 ~ wt + .302,
           pitch_outcome == 'S' & strikes_before == 2 ~ 0,
           is_in_play == T ~ xwt_ao,
           T ~ wt + .302
         ),
         xwt_oba = case_when(
           xwt_oba < 0 ~ 0,
           T ~ xwt_oba
         )) -> nu

nu %>%
  mutate(damage.avd = 1 - zone.damage,
         damage.prob = zone.bip * zone.damage,
         ZQ.dmg = (10 * zone.nrv) + (zone.bip * damage.avd),
         ZQ2 = (10 * zone.nrv) + (1 - damage.prob)) -> nu


nu %>%
  group_by(pitcher_mlbid, game_pk) %>%
  arrange(at_bat_index, pitch_of_ab) %>%
  group_by(game_pk, pitcher_mlbid, batter_mlbid) %>%
  mutate(times_faced = cumsum(!duplicated(at_bat_index))) %>%
  ungroup() -> nu

######### Now that data scraping and preprocessing is done, we can create new dfs required for app

nu %>%
  group_by(season) %>%
  mutate(lg_PA = sum(pitch_of_ab == 1 & level_id == 1, na.rm = T),
         lg_xwt_ao = mean(xwt_ao[is_in_play == T & level_id == 1], na.rm = T),
         lg_zswing = sum(swing == 1 & is_inzone_pi == T & level_id == 1, na.rm = T)/sum(is_inzone_pi == T & level_id == 1, na.rm = T),
         lg_chase = sum(swing == 1 & is_inzone_pi == F & level_id == 1, na.rm = T)/sum(is_inzone_pi == F & level_id == 1, na.rm = T),
         lg_z_o = 100 * (lg_zswing - lg_chase),
         lg_contact = 1 - (sum(whiff == 1 & level_id == 1, na.rm = T)/sum(swing == 1 & level_id == 1, na.rm = T)),
         lg.AVG = round(sum(level_id == 1 & pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2 & level_id == 1), 3),
         lg.SLG = round(sum(slg[level_id == 1 & is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S' & level_id == 1), 3),
         lg.ISO = lg.SLG - lg.AVG,
  ) %>%
  ungroup() %>%
  group_by(season, level_id) %>%
  mutate(
    lg.selection_tendency = 100 * sum(decision.value > 0 & swing == 0, na.rm = T)/sum(decision.value > 0, na.rm = T),
    lg.hittable_pitches_taken = 100 * sum(decision.value < 0 & swing == 0, na.rm = T)/sum(swing == 0, na.rm = T),
    lg.SEAGER = (lg.selection_tendency - lg.hittable_pitches_taken)) %>%
  ungroup() %>%
  group_by(batter_mlbid, level_id, season, hitting_code) %>%
  dplyr::summarise(
    hitter_name = sample(hitter_name, 1),
    G = length(unique(game_pk)),
    pitches = n(),
    selection_skill = round(100 * sum(decision.value > 0 & swing == 0, na.rm = T)/sum(decision.value > 0, na.rm = T), 1),
    hittable_pitches_taken = round(100 * sum(decision.value < 0 & swing == 0, na.rm = T)/sum(swing == 0, na.rm = T), 1),
    SEAGER = round((selection_skill - hittable_pitches_taken), 1),
    avg_decision_value = round(100 * mean(decision.value, na.rm = T), 2),
    SEAGER.plus = round(100 * SEAGER/sample(lg.SEAGER, 1), 1),
    lg.SEAGER = sample(lg.SEAGER, 1),
    #pHR = round(sum(hr.prob[is_in_play == T], na.rm = T), 1),
    HR = sum(pitch_outcome == 'HR', na.rm = T),
    outfield_pct = round(100 * sum(total_distance > 200, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    oppo_15_30 = round(100 * sum(launch_angle >= 10 & launch_angle <= 30 & spray_angle_adj > 0, na.rm = T)/sum(is_in_play==T, na.rm = T), 1),
    pull_FB_pct = round(100 * sum(launch_angle > 20 & spray_angle_adj < -15, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    xcontact = round(100 * (1 - mean(whiff.prob[swing == 1], na.rm = T)), 1),
    swing_pct = round(100 * sum(swing == 1, na.rm = T)/pitches, 1),
    contact_pct = round(100 * (1 - (sum(whiff == 1, na.rm = T)/sum(swing == 1, na.rm = T))), 1),
    contact_vs_avg = contact_pct - xcontact,
    secondary_whiff_pct = round(100 * sum(whiff == 1 & pitch_group != 'FA', na.rm = T)/sum(swing == 1 & pitch_group != 'FA', na.rm = T), 1),
    SwStr = round(100 * sum(whiff == 1, na.rm = T)/pitches, 1),
    uGB_swing = round(100 * sum(launch_angle <= -15, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_swing = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    loose_pull = round(100 * sum(spray_angle_adj <= -7 & launch_angle > 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    sdEV = sd(exit_velo[is_in_play == T], na.rm = T),
    sdLA = sd(launch_angle[is_in_play == T], na.rm = T),
    sdSA = sd(spray_angle_adj[is_in_play == T], na.rm = T),
    pull_tend = round(100 * sum(spray_angle_adj < -7 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    pull_FB_swing = round(100 * sum(launch_angle > 22 & spray_angle_adj < -20, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    FB_swing = round(100 * sum(launch_angle > 22 & launch_angle < 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    LD_swing = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_BIP = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    LD_BIP = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    Sac = sum(is_in_play == T & launch_angle > 25 & pitch_outcome == 'OUT' & base_change == T, na.rm = T),
    PA = length(unique(PA_id)),
    PA_G = PA/G,
    Platoon_PA = sum(platoon_adv == 1 & pitch_of_ab == 1, na.rm = T),
    Platoon_pct = round(100 * Platoon_PA/PA, 1),
    AB = PA - (sum(pitch_outcome == 'B' & balls_before == 3 | pitch_outcome == 'HBP', na.rm = T) + Sac),
    
    bbe = sum(is_in_play==T, na.rm = T),
    #phr_bbe = round(100*pHR/bbe, 2),
    bbe_pct = round(100 * bbe/PA, 1),
    hh_pct = round(100 * sum(exit_velo >= 95, na.rm = T)/bbe, 1),
    XBH = sum(pitch_outcome %in% c('2B', '3B', 'HR') & is_in_play == T, na.rm = T),
    damage_rate = round(100 * sum(damage==1, na.rm = T)/bbe, 1),
    damage_per_AB = round(100 *sum(damage==1, na.rm = T)/AB, 1),
    damage_per_PA = round(100 * sum(damage==1, na.rm = T)/PA, 1),
    xwt_ao = round(mean(xwt_ao[is_in_play == T], na.rm = T), 3),
    xwt_oba = round(mean(xwt_oba, na.rm = T), 3),
    xwtcon_plus = round(100 * xwt_ao/sample(lg_xwt_ao, 1), 1),
    avg_EV = round(mean(exit_velo[is_in_play == T], na.rm = T), 1),
    EV90th = round(quantile(exit_velo[is_in_play == T], probs = .9, na.rm = T), 1),
    max_EV = round(max(exit_velo[is_in_play == T], na.rm = T), 1),
    LA_10_35 = round(100 * sum(launch_angle >= 10 & launch_angle <= 35, na.rm = T) / bbe, 1),
    chase = round(100 * sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T), 1),
    z_con = round(100 * sum(whiff != 1 & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T & swing == 1, na.rm = T), 1),
    z_swing = round(100 * sum(swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T, na.rm = T), 1),
    z_minus_o = round(z_swing - chase, 1),
    K_pct = round(100 * sum(pitch_outcome == 'S' & strikes_before == 2)/PA, 1),
    BB_pct = round(100 * sum(pitch_outcome == 'B' & balls_before == 3)/PA, 1),
    whiff_rate = round(100 * sum(is_contact == F & swing_type == 'swing', na.rm = T)/sum(swing_type == 'swing', na.rm = T), 1),
    GB_rate = round(100 * sum(launch_angle < 10 & is_in_play == T, na.rm = T)/bbe, 1),
    FB_rate = round(100 * sum(launch_angle > 25 & is_in_play == T, na.rm = T)/bbe, 1),
    FBLD_pull = round(100 * sum(launch_angle >= 10 & launch_angle <= 40 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T & launch_angle >= 10 & launch_angle <= 40, na.rm = T), 1),
    sdLA = round(sd(launch_angle[is_in_play == T], na.rm = T), 1),
    avgLA_air = round(mean(launch_angle[is_in_play == T & launch_angle >= 10], na.rm = T), 1),
    avgEV_air = round(mean(exit_velo[is_in_play == T & launch_angle >= 20 & launch_angle <= 40], na.rm = T), 1),
    LA = round(mean(launch_angle[is_in_play == T], na.rm = T), 1),
    hr = sum(pitch_outcome == 'HR', na.rm = T),
    AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
    OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                      pitch_outcome == 'B' & balls_before == 3)/PA, 3),
    SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
    OPS = OBP + SLG,
    ISO = SLG - AVG,
    BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
    wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
    HR_per_PA = hr/PA,
    .groups = 'drop'
  ) %>%
  #filter(PA > 1) %>%
  relocate(c(AVG:ISO), .after = xwtcon_plus) %>%
  relocate(c(PA, ISO, BB_pct, OPS, SEAGER, avg_decision_value, EV90th, swing_pct, 
             contact_vs_avg, damage_rate, max_EV, hittable_pitches_taken, z_minus_o),
           .after = hitter_name) -> damage_df

damage_df %>%
  mutate(HR_per_PA = round(HR_per_PA, 4)) %>%
  relocate(AVG, .before = OPS) %>%
  relocate(c(xwt_ao,
             AB, bbe, HR_per_PA,
             damage_rate, damage_per_AB, xwtcon_plus), .after = PA) %>%
  relocate(c(SEAGER, damage_rate, SwStr, max_EV, EV90th, 
             hittable_pitches_taken, selection_skill), .after = bbe)-> damage_df

## attaching positions

# batters & positions tally dataframe
nu %>% 
  # filter(level_id == 1) %>%
  group_by(batter_mlbid, level_id, position, season) %>% 
  dplyr::summarise(
    games = length(unique(game_pk)),
    .groups = 'drop'
  ) %>% 
  filter(!is.na(position)) -> g #%>%
g %>%  
  filter(position != "") %>%
  group_by(batter_mlbid, level_id, season) %>%
  tidyr::pivot_wider(
    names_from = position, 
    values_from = games,
    values_fill = 0) %>%
  ungroup() -> positions


# join to damage df
positions %>%
  left_join(damage_df, by = c('batter_mlbid', 'level_id', 'season')) -> pos_df

# save
fwrite(pos_df, here("data", '2025_hitters.csv'))

###### Pitcher data
nu %>% 
  group_by(pitcher_mlbid, level_id, pitching_code, season) %>%
  mutate(G = length(unique(game_pk)),
         TBF = sum(pitch_of_ab == 1, na.rm = T),
         TBF_per_G = round(TBF/G, 1),
         SP_RP = case_when(TBF_per_G < 12 ~ 'RP',
                           TBF_per_G >= 12 ~ 'SP')) %>%
  ungroup() %>%
  group_by(season) %>%
  mutate(
    lg.zone.bip = mean(zone.bip[level_id == 1], na.rm = T),
    lg.damage.avd = mean(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.damage.avd.sd = sd(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.zone.nrv = mean(zone.nrv[level_id == 1], na.rm = T),
    lg.zone.nrv.sd = sd(zone.nrv[level_id == 1], na.rm = T),
    lg.zq.dmg = mean(ZQ.dmg[level_id == 1], na.rm = T),
    lg.zq.dmg.sd = sd(ZQ.dmg[level_id == 1], na.rm = T),
    lg.xwhiff = mean(whiff.prob[level_id == 1], na.rm = T),
    sd.xwhiff = sd(whiff.prob[level_id == 1], na.rm = T),
    std.xwhiff = ((((whiff.prob - lg.xwhiff)/sd.xwhiff))*.5)+1,
    std.zone.nrv = ((((zone.nrv - lg.zone.nrv)/lg.zone.nrv.sd))*.5)+1,
    std.dmg.avd = ((((damage.avd - lg.damage.avd)/lg.damage.avd.sd))*.5)+1,
    std.zq = ((((ZQ.dmg - lg.zq.dmg)/lg.zq.dmg.sd))*.5)+1) %>%
  ungroup() %>%
  group_by(pitcher_mlbid, level_id, season, pitcher_hand, pitching_code) %>%
  summarise(name = sample(pitcher_name, 1),
            role = sample(SP_RP, 1),
            pitches = n(),
            G = length(unique(game_pk)),
            IP = round(sum(outs_end - outs_start, na.rm = T)/3, 1),
            HBP = sum(pitch_outcome == 'HBP', na.rm = T),
            Walks = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            H_allowed = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'), na.rm = T),
            HR_allowed = sum(pitch_outcome == 'HR', na.rm = T),
            #pHR_allowed = sum(hr.prob[is_in_play == T], na.rm = T),
            #pHR_per_9 = round((pHR_allowed/IP) * 9, 2),
            RA = sum(runs_scored, na.rm = T),
            LOB_pct = round((H_allowed + Walks + HBP - RA)/(H_allowed + Walks + HBP - (1.4 * HR_allowed)), 3),
            TBF = sum(pitch_of_ab == 1, na.rm = T),
            TBF_per_G = round(TBF/G, 1),
            TBF_est = round((3.596 * IP) + (1.249 * RA) + (3.583 * LOB_pct), 0),
            baserunners = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') |
                                pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            IP_per_G = round(IP/G, 1),
            wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
            RA9 = round((RA/IP)*9, 3),
            WHIP = baserunners/IP,
            BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
            AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
            #n_AVG = round(mean(avg_n_1[is_in_play == T | pitch_outcome == 'S' & strikes_before == 2], na.rm = T), 3),
            OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                              pitch_outcome == 'B' & balls_before == 3)/sum(pitch_of_ab==1,na.rm = T), 3),
            SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
            OPS = OBP + SLG,
            ISO = SLG - AVG,
            pull_FB = round(100 * sum(launch_angle > 20 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
            fastball_velo = round(mean(pitch_velo[pitch_tag == main], na.rm = T), 1),
            max_velo = round(max(pitch_velo[pitch_group == 'FA'], na.rm = T), 1),
            fastball_vaa = round(mean(vaa[pitch_tag == main], na.rm = T), 2),
            main_fastball = sample(main, 1),
            rel_z = round(mean(release_z, na.rm = T), 1),
            rel_x = round(mean(release_x, na.rm = T), 1),
            ext = round(mean(ext, na.rm = T), 1),
            K = sum(pitch_outcome == 'S' & strikes_before==2,na.rm=T)/sum(pitch_of_ab==1,na.rm = T),
            BB = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T)/sum(pitch_of_ab==1, na.rm = T),
            K_BB = K-BB,
            GB_pct = sum(launch_angle < 0 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            IFFB_pct = sum(launch_angle > 40 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            HR = sum(pitch_outcome == 'HR', na.rm = T)/100,
            SwStr = sum(whiff==1,na.rm=T)/pitches,
            Whiff_Swing = sum(whiff==1, na.rm = T)/sum(swing==1, na.rm = T),
            Z_Contact = sum(is_contact == T & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(swing == 1 & is_inzone_pi == T, na.rm = T),
            Zone_pct = sum(is_inzone_pi == T, na.rm = T)/pitches,
            Chase = sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T),
            Ball_pct = sum(pitch_outcome == 'B', na.rm = T)/pitches,
            CSW = sum(whiff == 1 | pitch_outcome == 'S' & swing == 0, na.rm = T)/pitches,
            zone.nrv = mean(zone.nrv, na.rm = T),
            z.bip = median(zone.bip, na.rm = T),
            dmg.avd = mean(damage.avd[zone.bip >= z.bip], na.rm = T),
            zq.dmg = mean(ZQ.dmg,na.rm = T),          
            std.ZQ = mean(std.zq, na.rm = T),
            std.DMG = mean(std.dmg.avd[zone.bip >= z.bip], na.rm = T),
            std.NRV = mean(std.zone.nrv, na.rm = T),
            ZQ.dmg = zq.dmg/sample(lg.zq.dmg, 1),
            DMG.AVD = dmg.avd/sample(lg.damage.avd, 1),
            ZONE.NRV = zone.nrv/sample(lg.zone.nrv, 1),
            .groups = 'drop') %>%
  filter(TBF > 0) %>%
  arrange(desc(ZQ.dmg)) %>%
  mutate_at(vars(RA9:WHIP), ~round(., 2)) %>%
  mutate_at(vars(K:ZONE.NRV), ~ round(100 * ., 1)) %>%
  relocate(ZQ.dmg:ZONE.NRV, .after = IP) %>%
  relocate(c(K, K_BB, RA9, WHIP, ISO, wOBACON, std.ZQ:std.NRV), .after = IP) -> stuff_df

# save
fwrite(stuff_df, here("data",'2025_pitchers.csv'))


# individual pitches now
nu %>% 
  group_by(pitcher_mlbid, level_id, pitching_code, season) %>%
  mutate(G = length(unique(game_pk)),
         TBF = sum(pitch_of_ab == 1, na.rm = T),
         TBF_per_G = round(TBF/G, 1),
         SP_RP = case_when(TBF_per_G < 12 ~ 'RP',
                           TBF_per_G >= 12 ~ 'SP')) %>%
  ungroup() %>%
  group_by(season) %>%
  mutate(
    lg.zone.bip = mean(zone.bip[level_id == 1], na.rm = T),
    lg.damage.avd = mean(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.damage.avd.sd = sd(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.zone.nrv = mean(zone.nrv[level_id == 1], na.rm = T),
    lg.zone.nrv.sd = sd(zone.nrv[level_id == 1], na.rm = T),
    lg.zq.dmg = mean(ZQ.dmg[level_id == 1], na.rm = T),
    lg.zq.dmg.sd = sd(ZQ.dmg[level_id == 1], na.rm = T),
    lg.xwhiff = mean(whiff.prob[level_id == 1], na.rm = T),
    sd.xwhiff = sd(whiff.prob[level_id == 1], na.rm = T),
    std.xwhiff = ((((whiff.prob - lg.xwhiff)/sd.xwhiff))*.5)+1,
    std.zone.nrv = ((((zone.nrv - lg.zone.nrv)/lg.zone.nrv.sd))*.5)+1,
    std.dmg.avd = ((((damage.avd - lg.damage.avd)/lg.damage.avd.sd))*.5)+1,
    std.zq = ((((ZQ.dmg - lg.zq.dmg)/lg.zq.dmg.sd))*.5)+1) %>%
  ungroup() %>%
  group_by(pitcher_mlbid, level_id, season, pitching_code, pitcher_hand) %>%
  mutate(total = n()) %>%
  group_by(pitcher_mlbid, level_id, season, pitching_code, pitcher_hand, pitch_tag) %>%
  summarise(name = sample(pitcher_name, 1),
            role = sample(SP_RP, 1),
            pitches = n(),
            pct = round(100 * pitches/sample(total, 1), 1),
            TBF = sum(pitch_of_ab == 1, na.rm = T),
            wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
            BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
            AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
            OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                              pitch_outcome == 'B' & balls_before == 3)/sum(pitch_of_ab==1,na.rm = T), 3),
            SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
            OPS = OBP + SLG,
            ISO = SLG - AVG,
            velo = round(mean(pitch_velo, na.rm = T), 1),
            max_velo = round(max(pitch_velo, na.rm = T), 1),
            vaa = round(mean(vaa, na.rm = T), 2),
            haa = round(mean(haa_orig, na.rm = T), 2),
            ivb = round(mean(ivb, na.rm = T), 1),
            hb = round(mean(hb_orig, na.rm = T), 1),
            rel_z = round(mean(release_z, na.rm = T), 1),
            rel_x = round(mean(release_x, na.rm = T), 1),
            ext = round(mean(ext, na.rm = T), 1),
            K = sum(pitch_outcome == 'S' & strikes_before==2,na.rm=T)/sum(pitch_of_ab==1,na.rm = T),
            BB = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T)/sum(pitch_of_ab==1, na.rm = T),
            K_BB = K-BB,
            GB_pct = sum(launch_angle <= -10 & is_in_play == T, na.rm = T)/sum(pitch_of_ab == 1, na.rm = T),
            HR = sum(pitch_outcome == 'HR', na.rm = T)/100,
            Zone = sum(is_inzone_pi == T, na.rm = T)/pitches,
            SwStr = sum(whiff==1,na.rm=T)/pitches,
            Whiff_Swing = sum(whiff==1, na.rm = T)/sum(swing==1, na.rm = T),
            Z_Contact = sum(is_contact == T & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(swing == 1 & is_inzone_pi == T, na.rm = T),
            Chase = sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T),
            Ball_pct = sum(pitch_outcome == 'B', na.rm = T)/pitches,
            CSW = sum(whiff == 1 | pitch_outcome == 'S' & swing == 0, na.rm = T)/pitches,
            zone.nrv = mean(zone.nrv, na.rm = T),
            z.bip = median(zone.bip, na.rm = T),
            dmg.avd = mean(damage.avd[zone.bip >= z.bip], na.rm = T),
            zq.dmg = mean(ZQ.dmg,na.rm = T),
            #p.whiff = mean(whiff.p[swing == 1], na.rm = T),
            std.ZQ = mean(std.zq, na.rm = T),
            std.DMG = mean(std.dmg.avd[zone.bip >= z.bip], na.rm = T),
            std.NRV = mean(std.zone.nrv, na.rm = T),
            ZQ.dmg = zq.dmg/sample(lg.zq.dmg, 1),
            DMG.AVD = dmg.avd/sample(lg.damage.avd, 1),
            ZONE.NRV = zone.nrv/sample(lg.zone.nrv, 1),
            .groups = 'drop') %>%
  #filter(TBF > 1) %>%
  arrange(desc(ZQ.dmg)) %>%
  mutate_at(vars(K:ZONE.NRV), ~ round(100 * ., 1)) %>%
  relocate(ZQ.dmg:ZONE.NRV, .after = TBF) %>%
  relocate(c(SwStr, std.ZQ, Ball_pct, Chase, K, K_BB, ISO, wOBACON), .after = TBF) -> pitch_types_stuff_df

# save
fwrite(pitch_types_stuff_df, here("data",'2024_ind_pitches.csv'))

#### appending to the data sources for the app
## updating hitters

old_df <- fread(here("data_static",'damage_pos_until_2024.csv'))

pos_df %>%
  dplyr::select(UT, C, X1B, X2B, X3B, SS, OF, P, batter_mlbid, hitter_name, level_id,
         hitting_code, season, pitches, PA, bbe, damage_rate, EV90th, max_EV, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con, secondary_whiff_pct,
         contact_vs_avg
         #, adj_p_AVG, p_BABIP
         ) -> pos_df_1

old_df %>% 
  dplyr::select(UT, C, X1B, X2B, X3B, SS, OF, P, batter_mlbid, hitter_name, level_id, 
         hitting_code, season, pitches, PA, bbe, damage_rate, EV90th, max_EV, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con, secondary_whiff_pct,
         contact_vs_avg
         #, adj_p_AVG, p_BABIP
         ) -> old_df


new_df <- rbind(pos_df_1, old_df)

new_df$hitter_name <- iconv(new_df$hitter_name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(new_df, here("data",'damage_pos_2021_2024.csv'))

### pitchers

old_pitcher_df <- fread(here("data_static",'stuff_until_2024.csv')) 

old_pitcher_df %>%
  dplyr::select(level_id, TBF_per_G, pitcher_hand, pitcher_mlbid, name, season, pitching_code,
         pitches, TBF, IP, std.ZQ, std.DMG, std.NRV, 
         fastball_velo, max_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW, rel_z, rel_x, ext
         #, xERA9, xWHIP, xK
         ) -> old_pitcher_df

stuff_df %>%
  dplyr::select(level_id, TBF_per_G, pitcher_hand, pitcher_mlbid, name, season, pitching_code, 
         pitches, TBF, IP, std.ZQ, std.DMG, std.NRV, 
         fastball_velo, max_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW, rel_z, rel_x, ext
         #, xERA9, xWHIP, xK
         ) -> stuff_df_1

new_pitcher_df <- rbind(stuff_df_1, old_pitcher_df)

new_pitcher_df$name <- iconv(new_pitcher_df$name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(new_pitcher_df, here("data",'pitcher_stuff_new.csv'))

### pitch types

old_pitch_types <- fread(here("data_static",'pitch_types_until_2024.csv'))

old_pitch_types %>%
  dplyr::select(name, level_id, pitcher_mlbid, pitcher_hand, pitching_code, season, pitches, pitch_tag, pct,
         std.ZQ, std.DMG, std.NRV, velo, max_velo, vaa, haa, ivb, hb, SwStr, Z_Contact, Ball_pct, Zone,
         Chase, CSW) -> old_pitch_types

pitch_types_stuff_df %>%
  dplyr::select(name, level_id, pitcher_mlbid, pitcher_hand, pitching_code, season, pitches, pitch_tag, pct,
         std.ZQ, std.DMG, std.NRV, velo, max_velo, vaa, haa, ivb, hb, SwStr, Z_Contact, Ball_pct, Zone,
         Chase, CSW) -> pitch_types_new

pitch_types_new_df <- rbind(pitch_types_new, old_pitch_types)

pitch_types_new_df$name <- iconv(pitch_types_new_df$name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(pitch_types_new_df, here("data",'new_pitch_types.csv'))


##### constructing team averages and inputting them into the app
old_team_dmg <- fread(here("data_static",'team_damage_until_2024.csv'))

old_team_dmg %>%
  dplyr::select(hitting_code, level_id, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
         contact_vs_avg, secondary_whiff_pct) -> old_team_dmg

nu %>%
  group_by(season, level_id, hitting_code) %>%
  dplyr::summarise(
    G = length(unique(game_pk)),
    pitches = n(),
    selection_skill = round(100 * sum(decision.value > 0 & swing == 0, na.rm = T)/sum(decision.value > 0, na.rm = T), 1),
    hittable_pitches_taken = round(100 * sum(decision.value < 0 & swing == 0, na.rm = T)/sum(swing == 0, na.rm = T), 1),
    SEAGER = round((selection_skill - hittable_pitches_taken), 1),
    #pHR = round(sum(hr.prob[is_in_play == T], na.rm = T), 1),
    HR = sum(pitch_outcome == 'HR', na.rm = T),
    oppo_15_30 = round(100 * sum(launch_angle >= 10 & launch_angle <= 30 & spray_angle_adj > 0, na.rm = T)/sum(is_in_play==T, na.rm = T), 1),
    pull_FB_pct = round(100 * sum(launch_angle > 20 & spray_angle_adj < -15, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    xcontact = round(100 * (1 - mean(whiff.prob[swing == 1], na.rm = T)), 1),
    swing_pct = round(100 * sum(swing == 1, na.rm = T)/pitches, 1),
    contact_pct = round(100 * (1 - (sum(whiff == 1, na.rm = T)/sum(swing == 1, na.rm = T))), 1),
    contact_vs_avg = contact_pct - xcontact,
    SwStr = round(100 * sum(whiff == 1, na.rm = T)/pitches, 1),
    uGB_swing = round(100 * sum(launch_angle <= -15, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_swing = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    loose_pull = round(100 * sum(spray_angle_adj <= -7 & launch_angle > 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    sdEV = sd(exit_velo[is_in_play == T], na.rm = T),
    sdLA = sd(launch_angle[is_in_play == T], na.rm = T),
    sdSA = sd(spray_angle_adj[is_in_play == T], na.rm = T),
    pull_tend = round(100 * sum(spray_angle_adj < -7 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    pull_FB_swing = round(100 * sum(launch_angle > 22 & spray_angle_adj < -20, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    FB_swing = round(100 * sum(launch_angle > 22 & launch_angle < 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    LD_swing = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_BIP = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    LD_BIP = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    Sac = sum(is_in_play == T & launch_angle > 25 & pitch_outcome == 'OUT' & base_change == T, na.rm = T),
    PA = length(unique(PA_id)),
    PA_G = PA/G,
    Platoon_PA = sum(platoon_adv == 1 & pitch_of_ab == 1, na.rm = T),
    Platoon_pct = round(100 * Platoon_PA/PA, 1),
    AB = PA - (sum(pitch_outcome == 'B' & balls_before == 3 | pitch_outcome == 'HBP', na.rm = T) + Sac),
    
    bbe = sum(is_in_play==T, na.rm = T),
    #phr_bbe = round(100*pHR/bbe, 2),
    bbe_pct = round(100 * bbe/PA, 1),
    XBH = sum(pitch_outcome %in% c('2B', '3B', 'HR') & is_in_play == T, na.rm = T),
    damage_rate = round(100 * sum(damage==1, na.rm = T)/bbe, 1),
    EV90th = round(quantile(exit_velo[is_in_play == T], probs = .9, na.rm = T), 1),
    chase = round(100 * sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T), 1),
    z_con = round(100 * sum(whiff != 1 & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T & swing == 1, na.rm = T), 1),
    z_swing = round(100 * sum(swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T, na.rm = T), 1),
    z_minus_o = round(z_swing - chase, 1),
    secondary_whiff_pct = round(100 * sum(whiff == 1 & pitch_group != 'FA', na.rm = T)/sum(swing == 1 & pitch_group != 'FA', na.rm = T), 1),
    K_pct = round(100 * sum(pitch_outcome == 'S' & strikes_before == 2)/PA, 1),
    BB_pct = round(100 * sum(pitch_outcome == 'B' & balls_before == 3)/PA, 1),
    whiff_rate = round(100 * sum(is_contact == F & swing_type == 'swing', na.rm = T)/sum(swing_type == 'swing', na.rm = T), 1),
    GB_rate = round(100 * sum(launch_angle < 10 & is_in_play == T, na.rm = T)/bbe, 1),
    FB_rate = round(100 * sum(launch_angle > 25 & is_in_play == T, na.rm = T)/bbe, 1),
    FBLD_pull = round(100 * sum(launch_angle >= 10 & launch_angle <= 40 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T & launch_angle >= 10 & launch_angle <= 40, na.rm = T), 1),
    sdLA = round(sd(launch_angle[is_in_play == T], na.rm = T), 1),
    avgLA_air = round(mean(launch_angle[is_in_play == T & launch_angle >= 10], na.rm = T), 1),
    avgEV_air = round(mean(exit_velo[is_in_play == T & launch_angle >= 20 & launch_angle <= 40], na.rm = T), 1),
    LA = round(mean(launch_angle[is_in_play == T], na.rm = T), 1),
    hr = sum(pitch_outcome == 'HR', na.rm = T),
    AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
    OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                      pitch_outcome == 'B' & balls_before == 3)/PA, 3),
    SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
    OPS = OBP + SLG,
    ISO = SLG - AVG,
    BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
    wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
    HR_per_PA = hr/PA,
    baserunners = sum(pitch_outcome == 'B' & balls_before == 3 | pitch_outcome %in% c('1B', '2B', '3B', 'HR'), na.rm = T),
    outs = sum(outs_recorded, na.rm = T),
    IP = round(outs / 3, 1),
    BR_per_IP = round(baserunners / IP, 2),
    .groups = 'drop'
  ) %>%
  filter(PA > 1) %>%
  relocate(c(ISO, BB_pct, OPS, SEAGER, z_minus_o), .after = pitches) -> team_damage

team_damage %>%
  dplyr::select(hitting_code, level_id, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
         contact_vs_avg, secondary_whiff_pct) -> team_damage

new_team_damage <- rbind(team_damage, old_team_dmg)

fwrite(new_team_damage, here("data",'new_team_damage.csv'))

### now team pitching

old_team_stuff <- fread(here("data_static",'team_stuff_until_2024.csv'))

old_team_stuff %>%
  dplyr::select(level_id, season, pitching_code, IP, std.ZQ, std.DMG, std.NRV, 
         fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW #, xERA9, xWHIP, xK
         ) -> old_team_stuff

nu %>% 
  group_by(pitcher_mlbid, level_id, pitching_code, season) %>%
  mutate(G = length(unique(game_pk)),
         TBF = sum(pitch_of_ab == 1, na.rm = T),
         TBF_per_G = round(TBF/G, 1),
         SP_RP = case_when(TBF_per_G < 12 ~ 'RP',
                           TBF_per_G >= 12 ~ 'SP')) %>%
  ungroup() %>%
  group_by(season) %>%
  mutate(
    lg.zone.bip = mean(zone.bip[level_id == 1], na.rm = T),
    lg.damage.avd = mean(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.damage.avd.sd = sd(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.zone.nrv = mean(zone.nrv[level_id == 1], na.rm = T),
    lg.zone.nrv.sd = sd(zone.nrv[level_id == 1], na.rm = T),
    lg.zq.dmg = mean(ZQ.dmg[level_id == 1], na.rm = T),
    lg.zq.dmg.sd = sd(ZQ.dmg[level_id == 1], na.rm = T),
    std.zone.nrv = ((((zone.nrv - lg.zone.nrv)/lg.zone.nrv.sd))*.5)+1,
    std.dmg.avd = ((((damage.avd - lg.damage.avd)/lg.damage.avd.sd))*.5)+1,
    std.zq = ((((ZQ.dmg - lg.zq.dmg)/lg.zq.dmg.sd))*.5)+1) %>%
  ungroup() %>%
  group_by(pitching_code, level_id, season) %>%
  summarise(pitches = n(),
            G = length(unique(game_pk)),
            IP = round(sum(outs_end - outs_start, na.rm = T)/3, 1),
            HBP = sum(pitch_outcome == 'HBP', na.rm = T),
            Walks = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            H_allowed = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'), na.rm = T),
            HR_allowed = sum(pitch_outcome == 'HR', na.rm = T),
            #pHR_allowed = sum(hr.prob[is_in_play == T], na.rm = T),
            #pHR_per_9 = round((pHR_allowed/IP) * 9, 2),
            RA = sum(runs_scored, na.rm = T),
            LOB_pct = round((H_allowed + Walks + HBP - RA)/(H_allowed + Walks + HBP - (1.4 * HR_allowed)), 3),
            TBF = sum(pitch_of_ab == 1, na.rm = T),
            TBF_per_G = round(TBF/G, 1),
            TBF_est = round((3.596 * IP) + (1.249 * RA) + (3.583 * LOB_pct), 0),
            baserunners = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') |
                                pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            IP_per_G = round(IP/G, 1),
            wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
            RA9 = round((RA/IP)*9, 3),
            WHIP = baserunners/IP,
            BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
            AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
            #n_AVG = round(mean(avg_n_1[is_in_play == T | pitch_outcome == 'S' & strikes_before == 2], na.rm = T), 3),
            OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                              pitch_outcome == 'B' & balls_before == 3)/sum(pitch_of_ab==1,na.rm = T), 3),
            SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
            OPS = OBP + SLG,
            ISO = SLG - AVG,
            pull_FB = round(100 * sum(launch_angle > 20 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
            fastball_velo = round(mean(pitch_velo[pitch_tag == main], na.rm = T), 1),
            fastball_vaa = round(mean(vaa[pitch_tag == main], na.rm = T), 2),
            K = sum(pitch_outcome == 'S' & strikes_before==2,na.rm=T)/sum(pitch_of_ab==1,na.rm = T),
            BB = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T)/sum(pitch_of_ab==1, na.rm = T),
            K_BB = K-BB,
            GB_pct = sum(launch_angle < 0 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            IFFB_pct = sum(launch_angle > 40 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            HR = sum(pitch_outcome == 'HR', na.rm = T)/100,
            SwStr = sum(whiff==1,na.rm=T)/pitches,
            Whiff_Swing = sum(whiff==1, na.rm = T)/sum(swing==1, na.rm = T),
            Z_Contact = sum(is_contact == T & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(swing == 1 & is_inzone_pi == T, na.rm = T),
            Zone_pct = sum(is_inzone_pi == T, na.rm = T)/pitches,
            Chase = sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T),
            Ball_pct = sum(pitch_outcome == 'B', na.rm = T)/pitches,
            CSW = sum(whiff == 1 | pitch_outcome == 'S' & swing == 0, na.rm = T)/pitches,
            zone.nrv = mean(zone.nrv, na.rm = T),
            z.bip = median(zone.bip, na.rm = T),
            dmg.avd = mean(damage.avd[zone.bip >= z.bip], na.rm = T),
            zq.dmg = mean(ZQ.dmg,na.rm = T),          
            std.ZQ = mean(std.zq, na.rm = T),
            std.DMG = mean(std.dmg.avd[zone.bip >= z.bip], na.rm = T),
            std.NRV = mean(std.zone.nrv, na.rm = T),
            ZQ.dmg = zq.dmg/sample(lg.zq.dmg, 1),
            DMG.AVD = dmg.avd/sample(lg.damage.avd, 1),
            ZONE.NRV = zone.nrv/sample(lg.zone.nrv, 1),
            .groups = 'drop') %>%
  filter(TBF > 1) %>%
  arrange(desc(ZQ.dmg)) %>%
  mutate_at(vars(RA9:WHIP), ~round(., 2)) %>%
  mutate_at(vars(K:ZONE.NRV), ~ round(100 * ., 1)) %>%
  relocate(ZQ.dmg:ZONE.NRV, .after = IP) %>%
  relocate(c(K, K_BB, RA9, WHIP, ISO, wOBACON, std.ZQ:std.NRV), .after = IP) -> team_stuff

team_stuff %>%
  dplyr::select(level_id, season, pitching_code, IP, std.ZQ, std.DMG, std.NRV, 
         fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW #, xERA9, xWHIP, xK
         ) -> team_stuff

new_team_stuff <- rbind(team_stuff, old_team_stuff)

fwrite(new_team_stuff, here("data",'new_team_stuff.csv'))

#####

# constructing league avgs for app 
nu %>%
  group_by(season, level_id) %>%
  dplyr::summarise(
    G = length(unique(game_pk)),
    pitches = n(),
    selection_skill = round(100 * sum(decision.value > 0 & swing == 0, na.rm = T)/sum(decision.value > 0, na.rm = T), 1),
    hittable_pitches_taken = round(100 * sum(decision.value < 0 & swing == 0, na.rm = T)/sum(swing == 0, na.rm = T), 1),
    SEAGER = round((selection_skill - hittable_pitches_taken), 1),
    #pHR = round(sum(hr.prob[is_in_play == T], na.rm = T), 1),
    HR = sum(pitch_outcome == 'HR', na.rm = T),
    outfield_pct = round(100 * sum(total_distance > 200, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    oppo_15_30 = round(100 * sum(launch_angle >= 10 & launch_angle <= 30 & spray_angle_adj > 0, na.rm = T)/sum(is_in_play==T, na.rm = T), 1),
    pull_FB_pct = round(100 * sum(launch_angle > 20 & spray_angle_adj < -15, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    xcontact = round(100 * (1 - mean(whiff.prob[swing == 1], na.rm = T)), 1),
    swing_pct = round(100 * sum(swing == 1, na.rm = T)/pitches, 1),
    contact_pct = round(100 * (1 - (sum(whiff == 1, na.rm = T)/sum(swing == 1, na.rm = T))), 1),
    bip_pct = round(100*sum(is_in_play == T, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    contact_vs_avg = contact_pct - xcontact,
    SwStr = round(100 * sum(whiff == 1, na.rm = T)/pitches, 1),
    uGB_swing = round(100 * sum(launch_angle <= -15, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_swing = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    loose_pull = round(100 * sum(spray_angle_adj <= -7 & launch_angle > 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    sdEV = sd(exit_velo[is_in_play == T], na.rm = T),
    sdLA = sd(launch_angle[is_in_play == T], na.rm = T),
    sdSA = sd(spray_angle_adj[is_in_play == T], na.rm = T),
    pull_tend = round(100 * sum(spray_angle_adj < -7 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    pull_FB_swing = round(100 * sum(launch_angle > 22 & spray_angle_adj < -20, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    FB_swing = round(100 * sum(launch_angle > 22 & launch_angle < 43, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    LD_swing = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(swing == 1, na.rm = T), 1),
    uPU_BIP = round(100 * sum(launch_angle >= 43, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    LD_BIP = round(100 * sum(launch_angle > 0 & launch_angle <= 22, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
    Sac = sum(is_in_play == T & launch_angle > 25 & pitch_outcome == 'OUT' & base_change == T, na.rm = T),
    PA = length(unique(PA_id)),
    PA_G = PA/G,
    Platoon_PA = sum(platoon_adv == 1 & pitch_of_ab == 1, na.rm = T),
    Platoon_pct = round(100 * Platoon_PA/PA, 1),
    AB = PA - (sum(pitch_outcome == 'B' & balls_before == 3 | pitch_outcome == 'HBP', na.rm = T) + Sac),
    
    bbe = sum(is_in_play==T, na.rm = T),
    #phr_bbe = round(100*pHR/bbe, 2),
    bbe_pct = round(100 * bbe/PA, 1),
    XBH = sum(pitch_outcome %in% c('2B', '3B', 'HR') & is_in_play == T, na.rm = T),
    damage_rate = round(100 * sum(damage==1, na.rm = T)/bbe, 1),
    avg_EV = round(mean(exit_velo[is_in_play == T], na.rm = T), 1),
    EV90th = round(quantile(exit_velo[is_in_play == T], probs = .9, na.rm = T), 1),
    max_EV = round(max(exit_velo[is_in_play == T], na.rm = T), 1),
    chase = round(100 * sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T), 1),
    z_con = round(100 * sum(whiff != 1 & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T & swing == 1, na.rm = T), 1),
    z_swing = round(100 * sum(swing == 1 & is_inzone_pi == T, na.rm = T)/sum(is_inzone_pi == T, na.rm = T), 1),
    z_minus_o = round(z_swing - chase, 1),
    K_pct = round(100 * sum(pitch_outcome == 'S' & strikes_before == 2)/PA, 1),
    BB_pct = round(100 * sum(pitch_outcome == 'B' & balls_before == 3)/PA, 1),
    whiff_rate = round(100 * sum(is_contact == F & swing_type == 'swing', na.rm = T)/sum(swing_type == 'swing', na.rm = T), 1),
    GB_rate = round(100 * sum(launch_angle < 10 & is_in_play == T, na.rm = T)/bbe, 1),
    FB_rate = round(100 * sum(launch_angle > 25 & is_in_play == T, na.rm = T)/bbe, 1),
    FBLD_pull = round(100 * sum(launch_angle >= 10 & launch_angle <= 40 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T & launch_angle >= 10 & launch_angle <= 40, na.rm = T), 1),
    sdLA = round(sd(launch_angle[is_in_play == T], na.rm = T), 1),
    avgLA_air = round(mean(launch_angle[is_in_play == T & launch_angle >= 10], na.rm = T), 1),
    avgEV_air = round(mean(exit_velo[is_in_play == T & launch_angle >= 20 & launch_angle <= 40], na.rm = T), 1),
    LA = round(mean(launch_angle[is_in_play == T], na.rm = T), 1),
    hr = sum(pitch_outcome == 'HR', na.rm = T),
    AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
    OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                      pitch_outcome == 'B' & balls_before == 3)/PA, 3),
    SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
    OPS = OBP + SLG,
    ISO = SLG - AVG,
    BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
    wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
    HR_per_PA = hr/PA,
    .groups = 'drop'
  ) %>%
  filter(PA > 1) %>%
  relocate(c(ISO, BB_pct, OPS, SEAGER, z_minus_o), .after = pitches) -> lg_damage

# filter down to what's in the app
lg_damage %>%
  dplyr::select(level_id, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
         contact_vs_avg #, adj_p_AVG, p_BABIP
         ) -> lg_damage

# load old
old_lg_dmg <- fread(here("data_static",'hitting_lg_avg_until_2024.csv'))

old_lg_dmg %>%
  dplyr::select(level_id, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
         SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
         contact_vs_avg #, adj_p_AVG, p_BABIP
         ) -> old_lg_dmg

new_lg_dmg <- rbind(lg_damage, old_lg_dmg)
fwrite(new_lg_dmg, here("data",'new_hitting_lg_avg.csv'))

### league pitching
old_lg_stuff <- fread(here("data_static",'lg_stuff_until_2024.csv'))

nu %>%
  group_by(season) %>%
  mutate(
    lg.zone.bip = mean(zone.bip[level_id == 1], na.rm = T),
    lg.damage.avd = mean(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.damage.avd.sd = sd(damage.avd[level_id == 1 & zone.bip >= lg.zone.bip], na.rm = T),
    lg.zone.nrv = mean(zone.nrv[level_id == 1], na.rm = T),
    lg.zone.nrv.sd = sd(zone.nrv[level_id == 1], na.rm = T),
    lg.zq.dmg = mean(ZQ.dmg[level_id == 1], na.rm = T),
    lg.zq.dmg.sd = sd(ZQ.dmg[level_id == 1], na.rm = T),
    std.zone.nrv = ((((zone.nrv - lg.zone.nrv)/lg.zone.nrv.sd))*.5)+1,
    std.dmg.avd = ((((damage.avd - lg.damage.avd)/lg.damage.avd.sd))*.5)+1,
    std.zq = ((((ZQ.dmg - lg.zq.dmg)/lg.zq.dmg.sd))*.5)+1) %>%
  ungroup() %>%
  group_by(level_id, season) %>%
  summarise(pitches = n(),
            G = length(unique(game_pk)),
            IP = round(sum(outs_end - outs_start, na.rm = T)/3, 1),
            HBP = sum(pitch_outcome == 'HBP', na.rm = T),
            Walks = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            H_allowed = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'), na.rm = T),
            HR_allowed = sum(pitch_outcome == 'HR', na.rm = T),
            #pHR_allowed = sum(hr.prob[is_in_play == T], na.rm = T),
            #pHR_per_9 = round((pHR_allowed/IP) * 9, 2),
            RA = sum(runs_scored, na.rm = T),
            LOB_pct = round((H_allowed + Walks + HBP - RA)/(H_allowed + Walks + HBP - (1.4 * HR_allowed)), 3),
            TBF = sum(pitch_of_ab == 1, na.rm = T),
            TBF_per_G = round(TBF/G, 1),
            TBF_est = round((3.596 * IP) + (1.249 * RA) + (3.583 * LOB_pct), 0),
            baserunners = sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') |
                                pitch_outcome == 'B' & balls_before == 3, na.rm = T),
            IP_per_G = round(IP/G, 1),
            wOBACON = round(mean(woba[is_in_play == T], na.rm = T), 3),
            RA9 = round((RA/IP)*9, 3),
            WHIP = baserunners/IP,
            BABIP = round(sum(pitch_outcome %in% c('1B', '2B', '3B'), na.rm = T)/sum(is_in_play == T & pitch_outcome != 'HR', na.rm = T), 3),
            AVG = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR'))/sum(is_in_play == T | pitch_outcome == 'S' & strikes_before == 2), 3),
            #n_AVG = round(mean(avg_n_1[is_in_play == T | pitch_outcome == 'S' & strikes_before == 2], na.rm = T), 3),
            OBP = round(sum(pitch_outcome %in% c('1B', '2B', '3B', 'HR') | pitch_outcome == 'HBP' |
                              pitch_outcome == 'B' & balls_before == 3)/sum(pitch_of_ab==1,na.rm = T), 3),
            SLG = round(sum(slg[is_in_play == T], na.rm = T)/sum(is_in_play == T | strikes_before == 2 & pitch_outcome == 'S'), 3),
            OPS = OBP + SLG,
            ISO = SLG - AVG,
            pull_FB = round(100 * sum(launch_angle > 20 & spray_angle_adj < -20 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T), 1),
            fastball_velo = round(mean(pitch_velo[pitch_tag == main], na.rm = T), 1),
            fastball_vaa = round(mean(vaa[pitch_tag == main], na.rm = T), 2),
            K = sum(pitch_outcome == 'S' & strikes_before==2,na.rm=T)/sum(pitch_of_ab==1,na.rm = T),
            BB = sum(pitch_outcome == 'B' & balls_before == 3, na.rm = T)/sum(pitch_of_ab==1, na.rm = T),
            K_BB = K-BB,
            GB_pct = sum(launch_angle < 0 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            IFFB_pct = sum(launch_angle > 40 & is_in_play == T, na.rm = T)/sum(is_in_play == T, na.rm = T),
            HR = sum(pitch_outcome == 'HR', na.rm = T)/100,
            SwStr = sum(whiff==1,na.rm=T)/pitches,
            Whiff_Swing = sum(whiff==1, na.rm = T)/sum(swing==1, na.rm = T),
            Z_Contact = sum(is_contact == T & swing == 1 & is_inzone_pi == T, na.rm = T)/sum(swing == 1 & is_inzone_pi == T, na.rm = T),
            Zone_pct = sum(is_inzone_pi == T, na.rm = T)/pitches,
            Chase = sum(swing == 1 & is_inzone_pi == F, na.rm = T)/sum(is_inzone_pi == F, na.rm = T),
            Ball_pct = sum(pitch_outcome == 'B', na.rm = T)/pitches,
            CSW = sum(whiff == 1 | pitch_outcome == 'S' & swing == 0, na.rm = T)/pitches,
            zone.nrv = mean(zone.nrv, na.rm = T),
            z.bip = median(zone.bip, na.rm = T),
            dmg.avd = mean(damage.avd[zone.bip >= z.bip], na.rm = T),
            zq.dmg = mean(ZQ.dmg,na.rm = T),          
            std.ZQ = mean(std.zq, na.rm = T),
            std.DMG = mean(std.dmg.avd[zone.bip >= z.bip], na.rm = T),
            std.NRV = mean(std.zone.nrv, na.rm = T),
            ZQ.dmg = zq.dmg/sample(lg.zq.dmg, 1),
            DMG.AVD = dmg.avd/sample(lg.damage.avd, 1),
            ZONE.NRV = zone.nrv/sample(lg.zone.nrv, 1),
            .groups = 'drop') %>%
  filter(TBF > 1) %>%
  arrange(desc(ZQ.dmg)) %>%
  mutate_at(vars(RA9:WHIP), ~round(., 2)) %>%
  mutate_at(vars(K:ZONE.NRV), ~ round(100 * ., 1)) %>%
  relocate(ZQ.dmg:ZONE.NRV, .after = IP) %>%
  relocate(c(K, K_BB, RA9, WHIP, ISO, wOBACON, std.ZQ:std.NRV), .after = IP) -> lg_stuff

lg_stuff %>%
  dplyr::select(level_id, season, fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW #, xERA9, xWHIP, xK
         ) -> lg_stuff

old_lg_stuff %>%
  dplyr::select(level_id, season, fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
         Chase, CSW #, xERA9, xWHIP, xK
         ) -> old_lg_stuff

new_lg_stuff <- rbind(lg_stuff, old_lg_stuff)
fwrite(new_lg_stuff, here("data",'new_lg_stuff.csv'))


##### clear unnecessary models and pulls from environment
rm(bb, outs, ask_mlb, team_ids, test, pHR_pred, phr_bbe_predictor, hit_composite, K_pred, WHIP_pred2, hr_predict, damage_mod, BABIP_predict,
   adj_AVG, ask, zone.bip, zone.damage, zone.rv, xgb.lwt, xgb.woba, whiff.prob, RA_pred, get_game_pks, bbe_predict, BB_pred, avg_next, con,
   bam.xwt.gain)

# add in percentiles
# percentile function 
pctile <- function(x){
  y = round((rank(x) - 1) / (n() - 1) * 100, 0)
  
  return(y)
  
}

# hitters percentiles
new_df %>%
  filter(pitches >= 100 & bbe >= 20) %>%
  group_by(level_id, season) %>%
  mutate(SEAGER_pctile = ifelse(pitches >= 100, ntile(SEAGER, n = 100), NA),
         selection_pctile = ifelse(pitches >= 100, ntile(selection_skill, n = 100), NA),
         hittable_pitches_pctile = ifelse(pitches >= 100, 100 - ntile(hittable_pitches_taken, n = 100), NA),
         damage_pctile = ifelse(bbe >= 20, ntile(damage_rate, n = 100), NA),
         EV90_pctile = ifelse(bbe >= 20, ntile(EV90th, n = 100), NA),
         max_pctile = ifelse(bbe >= 20, ntile(max_EV, n = 100), NA),
         pfb_pctile = ifelse(bbe >= 20, ntile(pull_FB_pct, n = 100), NA),
         chase_pctile = ifelse(pitches >= 100, 100 - ntile(chase, n = 100), NA),
         z_con_pctile = ifelse(pitches >= 100, ntile(z_con, n = 100), NA),
         sec_whiff_pctile = ifelse(pitches >= 100, 100 - ntile(secondary_whiff_pct, n = 100), NA),
         c_vs_avg_pctile = ifelse(pitches >= 100, ntile(contact_vs_avg, n = 100), NA)
  ) %>%
  dplyr::select(batter_mlbid, 
         hitter_name, season, level_id, hitting_code, 
         SEAGER_pctile:c_vs_avg_pctile, pitches, bbe, PA) -> new_df_2


new_df_2$hitter_name <- iconv(new_df_2$hitter_name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(new_df_2, here("data",'hitter_pctiles.csv'))


# pitcher percentiles
new_pitcher_df %>%
  filter(pitches >= 100) %>%
  group_by(level_id, season) %>%
  mutate(PQ_pctile = ntile(std.ZQ, 100),
         DMG_pctile = ntile(std.DMG, 100),
         NRV_pctile = ntile(std.NRV, 100),
         FA_velo_pctile = ntile(fastball_velo, 100),
         FA_max_pctile = ntile(max_velo, 100),
         FA_vaa_pctile = 100 - ntile(fastball_vaa, 100),
         SwStr_pctile = ntile(SwStr, 100),
         Ball_pctile = 100 - ntile(Ball_pct, 100),
         Z_con_pctile = 100 - ntile(Z_Contact, 100),
         Chase_pctile = ntile(Chase, 100),
         CSW_pctile = ntile(CSW, 100),
         rZ_pctile = ntile(rel_z, 100),
         rX_pctile = ntile(abs(rel_x), 100),
         ext_pctile = ntile(ext, 100)
  ) %>%
  dplyr::select(pitcher_mlbid,
         name, season, level_id, pitching_code,
         PQ_pctile:ext_pctile, TBF, IP) -> new_pitcher_df_2

new_pitcher_df_2$name <- iconv(new_pitcher_df_2$name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(new_pitcher_df_2, here("data",'pitcher_pctiles.csv'))


# individual pitch type percentiles
pitch_types_new_df %>%
  filter(pitches >= 50) %>%
  group_by(pitch_tag, season, level_id) %>%
  mutate(usage_pctile = pctile(pct),
         PQ_pctile = pctile(std.ZQ),
         DMG_pctile = pctile(std.DMG),
         NRV_pctile = pctile(std.NRV),
         velo_pctile = pctile(velo),
         max_velo_pctile = pctile(max_velo),
         vaa_pctile = case_when(pitch_tag %in% c('FA', 'HC') ~ 100 - pctile(vaa), 
                                T ~ pctile(vaa)),
         haa_pctile = pctile(abs(haa)),
         ivb_pctile = case_when(pitch_tag %in% c('FA', 'HC') ~ 100 - pctile(ivb),
                                T ~ pctile(ivb)),
         hb_pctile = pctile(abs(hb)),
         SwStr_pctile = pctile(SwStr),
         Ball_pctile = 100 - pctile(Ball_pct),
         zone_pctile = pctile(Zone),
         Z_con_pctile = 100 - pctile(Z_Contact),
         Chase_pctile = pctile(Chase),
         CSW_pctile = pctile(CSW),
  ) %>%
  dplyr::select(pitcher_mlbid,
         name, season, level_id, pitching_code, pitch_tag, pitcher_hand,
         usage_pctile:CSW_pctile, pitches) -> pitch_types_new_df_2

pitch_types_new_df_2$name <- iconv(pitch_types_new_df_2$name, from = 'UTF-8', to = 'ASCII//TRANSLIT')

fwrite(pitch_types_new_df_2, here("data",'pitch_types_pctiles.csv'))
