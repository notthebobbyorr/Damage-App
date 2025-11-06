#
# Change date back once back in town
#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(shinythemes)
library(shinyscreenshot)
library(shinyWidgets)
library(dplyr)
library(data.table)
library(mgcv)
library(DT)
library(bslib)
library(gridExtra)
library(ggcorrplot)


# load data

#damage_df <- read.csv('damage_positions_2021_2023.csv')

damage_df <- read.csv('damage_pos_2021_2024.csv')

damage_df %>% filter(season != "NA") -> damage_df

hitter_pct <- read.csv('hitter_pctiles.csv')

#pitcher_df <- read.csv('pitcher_stuff.csv') #read.csv('MyStuff_2020_2023.csv')

pitcher_df <- read.csv('pitcher_stuff_new.csv')

pitcher_pct <- read.csv('pitcher_pctiles.csv')

#hitting_avg <- read.csv('hitting_lg_avgs.csv')

hitting_avg <- read.csv('new_hitting_lg_avg.csv')

#pitching_avg <- read.csv('lg_stuff.csv') #read.csv('pitching_lg_avgs.csv')

pitching_avg <- read.csv('new_lg_stuff.csv')

#team_damage <- read.csv('team_damage.csv')

team_damage <- read.csv('new_team_damage.csv')

#team_stuff <- read.csv('team_stuff.csv')

team_stuff <- read.csv('new_team_stuff.csv')

#pitch_types <- read.csv('pitch_type_stuff.csv')  #read.csv('pitch_types.csv')

pitch_types <- fread('new_pitch_types.csv')

pitch_types %>%
  filter(!is.na(season)) %>%
  mutate(pitch_group = case_when(pitch_tag %in% c('FA', 'HC', 'SI') ~ 'FA',
                                 pitch_tag %in% c('SL', 'SW', 'CU') ~ 'BR',
                                 pitch_tag %in% c('CH', 'FS') ~ 'OFF',
                                 T ~ 'OTHER')) -> pitch_types

pitch_types_pct <- fread('pitch_types_pctiles.csv')

hitting_cor <- read.csv('hitting_cor.csv')

pitching_cor <- read.csv('pitching_cor2.csv')

PA_est <- readRDS('PA_estimate.rds')
R_est <- readRDS('runs_per_pa.rds')
RBI_est <- readRDS('rbi_per_pa.rds')


ui <- navbarPage(title = "Profiles",
                 id = 'Profiles',
                 #theme = bs_theme(bootswatch = "lux"), 
                 theme = shinytheme("cyborg"),
                 tags$head(
                   tags$style(
                     HTML("body {
                          color: white;
                     }
                          "))
                 ),
                 tags$style(HTML("
                    .dataTables_wrapper {
                    color: white;
                    }
                    
                    .dataTables_length select {
                           color: #000;
                           background-color: white
                           }
                    ")),
                # tags$head(
                #   tags$style(
                #     HTML(
                #       ".nav-tabs > li > a { font-family: 'Verdana', sans-serif; }",
                #       ".nav-tabs > li > a { font-size: 10px; }",
                #       ".nav-tabs > li > a { padding: 4px; }",
                #       ".nav-tabs > li > a { background-color: white; }",
                #       ".nav-tabs > li.active > a, .nav-tabs > li.active > a:focus, .nav-tabs > li.active > a:hover { background-color: #75B2FF; }",
                #       ".nav-tabs > li.active > a, .nav-tabs > li.active > a:focus, .nav-tabs > li.active > a:hover { color: white; }",
                #       ".nav-tabs > li > a { border: 1px solid #337ab7; }",
                #       ".nav-tabs { flex-wrap: wrap; }",
                #       ".nav-tabs > li { width: 25%; text-align: center; }"
                #     )
                #   )
                # ),
#                 tags$style("
#               body {
#     -moz-transform: scale(0.80, 0.80); /* Moz-browsers */
#     zoom: 0.80; /* Other non-webkit browsers */
#     zoom: 80%; /* Webkit browsers */
# }
#               "),

                  navbarMenu('Options',
                             tabPanel('Welcome Page',
                                      h3('Welcome to My App'),
                                      p(HTML("Here you will find metrics I (https://twitter.com/NotTheBobbyOrr) have developed for analyzing hitters & pitchers at a player and team level. 
                                      I make frequent use of these statistics in my work at BaseballProspectus dot com (https://www.baseballprospectus.com/author/ringtheodubel/) and for my own fantasy strategy. You
                                             can navigate via the dropdown bar and there are glossaries containing explanations for each statistic at the bottom of the dropdown. The
                                             data is from 2021-present, with an eye towards updates and upgrades throughout each successive season. Most of the pages contain data for each level
                                             that had statcast data in that season. I hope you find this useful, or--at the very least--entertaining. Enjoy!")),
                                      p(HTML("Tip jar if you're feeling generous:")),
                                      p(HTML("Venmo: @Robert-Orr7")),
                                      p(HTML("Paypal: orrrobf @ gmail dot com")),
                                      p(HTML('Feedback: If you have any suggestions or just want to say hi, shoot me a DM on Twitter or send me an email at orrrobf @ gmail dot com.')),
                                      p(HTML('Last Update:'), Sys.Date())
                                      ),
                              tabPanel('Hitters',
                                       fluidRow(
                                         sidebarPanel(
                                                selectInput("level", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A"), selected = "MLB"),
                                                selectizeInput("season", "Select Season:", choices = c("All", (unique(damage_df$season))), selected = "2025", multiple = TRUE),
                                                sliderInput("min_value", "Minimum Value:", min = 0, max = 500, value = 1),
                                                selectInput("value_type", "Filter By:", choices = c("PA", "BBE"), selected = "BBE"),
                                                selectizeInput("team", "Select Team:", choices = c("All", sort(unique(damage_df$hitting_code))), selected = "All", multiple = TRUE),
                                                selectizeInput("player", "Select Player:", choices = c("All", sort(c(unique(damage_df$hitter_name)))), selected = "All", multiple = TRUE),
                                                # Multi-select dropdown for positions
                                                pickerInput(
                                                  "positions",
                                                  "Select Positions:",
                                                  choices = c('C', '1B', '2B', 'SS', '3B', 'OF', 'UT', 'P'),  # Extract unique positions from game played columns
                                                  options = list(`actions-box` = TRUE),
                                                  multiple = TRUE
                                                ),
                                                pickerInput(
                                                  "position_group",
                                                  "Select Position Group:",
                                                  choices = c('C', 'MI', 'CI', 'OF'),  # Extract unique positions from "eligible" column
                                                  options = list(`actions-box` = TRUE),
                                                  multiple = TRUE
                                                ),
                                                screenshotButton(label = "Snap!", id = "hitter_table",
                                                                 scale=2)),
                                         mainPanel( 
                                                DTOutput("hitter_table"))
                                       )),
                             tabPanel('Hitters - Percentiles',
                                      h3('Percentile Rankings - Hitters'),
                                      p(HTML("min. 100 pitches seen & 20 batted balls at respective level")),
                                      fluidRow(
                                        sidebarPanel(
                                          selectInput("level_h_pct", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A"), selected = "MLB"),
                                          selectizeInput("season_pct", "Select Season:", choices = c("All", (unique(hitter_pct$season))), selected = "2025", multiple = TRUE),
                                          selectizeInput("team_pct", "Select Team:", choices = c("All", sort(unique(hitter_pct$hitting_code))), selected = "All", multiple = TRUE),
                                          selectizeInput("player_pct", "Select Player:", choices = c("All", sort(c(unique(hitter_pct$hitter_name)))), selected = "All", multiple = TRUE),
                                          screenshotButton(label = "Snap!", id = "hitter_pcts",
                                                           scale=2)),
                                        mainPanel( 
                                          DTOutput("hitter_pcts"))
                                      )),
                              tabPanel('Pitchers',
                                       fluidRow(
                                         sidebarPanel(
                                                selectInput("lg_level", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A", 'Low Minors'), selected = "MLB"),
                                                selectizeInput("year", "Select Season:", choices = c("All", unique(pitcher_df$season)), selected = "2025", multiple = TRUE),
                                                sliderInput("min", "Minimum Value:", min = 0, max = 1000, value = 10),
                                                selectInput("filter_type", "Filter By:", choices = c("IP", "TBF"), selected = "TBF"),
                                                sliderInput("per_game", "Filter TBF per Game:", min = 0, max = 30, value = c(0, 30)),
                                                selectInput("pitcher_hand", "Select Pitcher Hand:", choices = c("Both", "LHP", "RHP"), selected = "Both"),
                                                selectizeInput("Team", "Select Team:", choices = c("All", sort(unique(pitcher_df$pitching_code))), selected = "All", multiple = TRUE),
                                                selectizeInput("pitcher", "Select Pitcher:", choices = c("All", sort(c(unique(pitcher_df$name)))), selected = "All", multiple = TRUE),
                                                screenshotButton(label = "Snap!", id = "pitcher_table",
                                                                 scale=2)),
                                         mainPanel(
                                                DTOutput("pitcher_table")))),
                             tabPanel('Pitchers - Percentiles',
                                      h3('Percentile Rankings - Pitchers'),
                                      p(HTML("min. 100 pitches thrown at respective level")),
                                      fluidRow(
                                        sidebarPanel(
                                          selectInput("level_p_pct", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A"), selected = "MLB"),
                                          selectizeInput("season_pct_p", "Select Season:", choices = c("All", (unique(pitcher_pct$season))), selected = "2025", multiple = TRUE),
                                          selectizeInput("team_pct_p", "Select Team:", choices = c("All", sort(unique(pitcher_pct$pitching_code))), selected = "All", multiple = TRUE),
                                          selectizeInput("player_pct_p", "Select Player:", choices = c("All", sort(c(unique(pitcher_pct$name)))), selected = "All", multiple = TRUE),
                                          screenshotButton(label = "Snap!", id = "pitcher_pcts",
                                                           scale=2)),
                                        mainPanel( 
                                          DTOutput("pitcher_pcts"))
                                      )),
                              tabPanel('Individual Pitches',
                                       fluidRow(
                                         sidebarPanel(
                                                selectInput("pitch_tag_level", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A", "Low Minors"), selected = "MLB"),
                                                selectizeInput("pitch_tag_year", "Select Season:", choices = c("All", unique(pitch_types$season)), selected = "2025", multiple = TRUE),
                                                selectInput("pitch_tag_hand", "Select Pitcher Hand:", choices = c("Both", "LHP", "RHP"), selected = "Both"),
                                                selectizeInput("pitch_tag_team", "Select Team:", choices = c("All", sort(unique(pitch_types$pitching_code))), selected = "All", multiple = TRUE),
                                                selectizeInput("pitch_tag_pitcher", "Select Pitcher:", choices = c("All", sort(unique(pitch_types$name))), selected = "All", multiple = TRUE),
                                                #selectInput("platoon", "Select Platoon State:", choices = c("Both", "Same-Handed", "Oppo-Handed"), selected = "Both"),
                                                selectizeInput("pitch_group", "Select Pitch Group:", choices = c("All", sort(c(unique(pitch_types$pitch_group)))), selected = "All", multiple = TRUE),
                                                selectizeInput("pitch_tag", "Select Pitch Type:", choices = c("All", sort(c(unique(pitch_types$pitch_tag)))), selected = "All", multiple = TRUE),
                                                sliderInput("pitch_tag_min", "Minimum Pitches:", min = 0, max = 3000, value = 10)),
                                         mainPanel(
                                                DTOutput("pitch_types_table"))
                                       )),
                             tabPanel('Individual Pitches - Percentiles',
                                      h3('Percentile Rankings - Pitch Types'),
                                      p(HTML('min. 50 pitches thrown. Percentiles are within pitch type at respective level.')),
                                      fluidRow(
                                        sidebarPanel(
                                          selectInput("pitch_tag_level_pct", "Select Level:", choices = c("All", "MLB", "Triple-A", "Low-A", "Low Minors"), selected = "MLB"),
                                          selectizeInput("pitch_tag_year_pct", "Select Season:", choices = c("All", unique(pitch_types_pct$season)), selected = "2025", multiple = TRUE),
                                          selectInput("pitch_tag_hand_pct", "Select Pitcher Hand:", choices = c("Both", "LHP", "RHP"), selected = "Both"),
                                          selectizeInput("pitch_tag_team_pct", "Select Team:", choices = c("All", sort(unique(pitch_types_pct$pitching_code))), selected = "All", multiple = TRUE),
                                          selectizeInput("pitch_tag_pitcher_pct", "Select Pitcher:", choices = c("All", sort(unique(pitch_types_pct$name))), selected = "All", multiple = TRUE),
                                          selectizeInput("pitch_tag_pct", "Select Pitch Type:", choices = c("All", sort(c(unique(pitch_types_pct$pitch_tag)))), selected = "All", multiple = TRUE),
                                        mainPanel(
                                          DTOutput("pitch_types_table_pct"))
                                      ))
                             ),
                              tabPanel('Team Hitting',
                                       fluidRow(
                                         sidebarPanel(
                                           selectizeInput("team_hit_level", "Select Level:", choices = c("MLB", "Triple-A", "Low-A", "Low Minors"), selected = "MLB", multiple = F),
                                                selectizeInput("team_hitting_season", "Select Season:", choices = c("All", unique(team_damage$season)), selected = "2025", multiple = TRUE),
                                                selectizeInput("team_hitting", 'Select Team', choices = c("All", sort(c(unique(team_damage$hitting_code)))), selected = "All", multiple = TRUE)),
                                         mainPanel(
                                                DTOutput("team_hitting_table"))
                                       )),
                              tabPanel('Team Pitching',
                                       fluidRow(
                                         sidebarPanel(
                                           selectizeInput("team_pitch_level", "Select Level:", choices = c("MLB", "Triple-A", "Low-A", "Low Minors"), selected = "MLB", multiple = F),
                                                selectizeInput("team_pitching_season", "Select Season:", choices = c("All", unique(team_stuff$season)), selected = "2025", multiple = TRUE),
                                                selectizeInput("team_pitching", 'Select Team', choices = c("All", sort(c(unique(team_stuff$pitching_code)))), selected = "All", multiple = TRUE)),
                                         mainPanel(
                                                DTOutput("team_pitching_table"))
                                       )),
                              tabPanel('League Averages - Hitting',
                                       fluidRow(
                                         sidebarPanel(
                                                selectizeInput("hitting_szn", "Select Season:", choices = c("All", unique(hitting_avg$season)), selected = "2025", multiple = TRUE)),
                                         mainPanel(
                                                DTOutput("lg_hitting_table"))
                                       )),
                              tabPanel('League Averages - Pitching',
                                       fluidRow(
                                         sidebarPanel(
                                           selectizeInput("lgs_level", "Select Level:", choices = c("MLB", "Triple-A", "Low-A", "Low Minors"), selected = "MLB", multiple = F),
                                                selectizeInput("pitching_szn", "Select Season:", choices = c("All", unique(pitching_avg$season)), selected = "2025", multiple = TRUE)),
                                         mainPanel(
                                                DTOutput("lg_pitching_table"))
                                       )),
                              tabPanel('Glossary - Hitting',
                                       h3('Hitting Metrics'),
                                       p(HTML("<strong>Damage</strong> - A batted ball that clears a threshold of exit velocity, launch angle, and hit direction
                                            likely to produce an XBH. Tracked per batted ball. Intro piece: https://thesacbunt.home.blog/2022/01/10/a-different-way-to-evaluate-hitters/")),
                                       p(HTML("<strong>Pulled FB (%)</strong> - The percentage of a player's batted balls hit at a launch angle above 20 degrees and a 
                                              spray angle of 15 degrees or greater to their pull side.")),
                                       p(HTML("<strong>SEAGER</strong> - <strong>SE</strong>lective <strong>AG</strong>gression <strong>E</strong>ngagement <strong>R</strong>ate. 
                                            The difference between <strong>Selection Tendency</strong> and <strong>Hittable Pitches Taken</strong>. Reflects how well a player balances
                                              attacking good pitches to hit with laying off unfavorable pitches. Intro piece: https://www.baseballprospectus.com/news/article/86572/the-crooked-inning-corey-seager-rangers/")),
                                       p(HTML("<strong>Selection Tendency</strong> - How many of a player's good decisions (positive expected value) were a result of taking pitches. This can be thought of as: Good Takes / Good Decisions")),
                                       p(HTML("<strong>Hittable Pitches Taken</strong> - How many of a player's takes were pitches with a positive expected value, aka hittable pitches. This can be thought of as: Bad Takes / Total Takes")),
                                       p(HTML("Whiff vs. Secondaries - The whiff per swing r`ate of a player against breaking & offspeed pitches.")),
                                       p(HTML("<strong>Contact Over Expected</strong> - A player's contact rate compared to their expected contact rate given the quality of
                                              the pitches they swing at."),
                                         
                                      # p(HTML("<strong>Skill-Based AVG</strong> - A player's batting average adjusted for their bat-to-ball skills,
                                      #        quality of pitches swung at, and batted ball mix.")),
                                      # p(HTML("<strong>Skill-Based BABIP</strong> - A player's batting average on balls in play adjusted for their batted ball traits,
                                      # specifically their launch angle distribution & consistency and their pulled batted ball tendency."))
                                       )
                              ),
                              tabPanel('Glossary - Pitching',
                                       h3('Pitching Metrics'),
                                       p(HTML("<strong>Pitch Quality</strong> - A blend of <strong>Non-BIP Skill</strong> and <strong>Damage Suppression</strong>, each weighted by that pitcher's tendency to
                                              allow or avoid balls in play. Scaled so that a league average Pitch Quality is 100 with a standard deviation of 50, and anything above 100 indicates better than average overall Pitch Quality.")),
                                       p(HTML("<strong>Non-BIP Skill</strong> - Expected run values of outcomes on non-balls in play (foul, whiff, called strike, called ball) based on a pitch & pitcher's physical characteristics only.
                                              Does not consider location. Scaled so that a league average spread of outcomes in that season is a 100, and anything above 100  with a standard deviation of 50, indicates better than average expected outcomes for the pitcher. This measure correlates most closely with a pitcher's ability to miss bats.")),
                                       p(HTML("<strong>Damage Suppression</strong> - Ability to avoid Damage on balls in play based on a pitch & pitcher's physical characteristics only. 
                                              Does not consider location. Scaled so that a league average rate of damage suppression in that season is a 100  with a standard deviation of 50, and anything above 100 indicates a better ability to avoid extra base damage for the pitcher. This measure correlates closely with limiting opposing ISO, AKA contact management.")),
                                       #p(HTML("PQ-Based ERA - Pitch Quality-adjusted ERA")),
                                       #p(HTML("PQ-Based WHIP - Baserunners allowed per inning based on pitch qualities and expected outcomes on batted balls. Includes a regressed BB% that considers pitch traits.")),
                                       #p(HTML("PQ-Based K (%) - Strikeout percentage adjusted for non-BIP pitch qualities. Doesn't consider pitch locations.")),
                                      h2('Pitch Model Background'),
                                       p(HTML("The pitch model consists of a pair of sub-models, one for Balls in Play (BIP) and one for Non-BIP.
                                       <br>
                                 
                                              <br>
                                              The features considered by each model are: <br> Velocity, IVB, HB, VAA, HAA, Vertical Angle at Release, Horizontal Angle at Release,
                                              Spin Efficiency, Spin Axis, Axis Differential (aka Seam-Shifted Wake), RPM, Release Height, Release Width & Extension, Pitcher Hand, and Batter Hand <br>
                                              <br>
                                              Each model also accounts for the deltas that a pitcher's secondary pitches have from that pitcher's most-used fastball for the following traits: <br>Velocity, IVB, HB, VAA, HAA,
                                              Vertical Angle at Release, & Horizontal Angle at Release"))),
                             tabPanel('PA & R/RBI Calculator',
                                      fluidRow(
                                        sidebarPanel(
                                          textInput('player_games', 'Enter # of Games Played', value = 150),
                                          textInput('team_OPS', 'Enter Team OPS', value = .734),
                                          selectInput('lineup_spot', 'Enter Spot in the Order', choices = seq(1, 9, by = 1)),
                                          selectInput("role", "Pick Platoon Role", choices = c('vs RHP Only', 'vs LHP Only', 'vs Both'), selected = 'vs Both')
                                          )),
                                      mainPanel(
                                        DTOutput("counting_stats"))
                                      ),
                             tabPanel('Hitting Correlations',
                                      
                                      p(HTML("Weighted Correlations of key metrics on current and next season statistics among hitters with 200+ PA in consecutive seasons. Data from 2021-2023 seasons.")),
                                      p(HTML("A value closer to 1 or -1 indicates a strong linear relationship between 2 variables. 
                                             Values closer to 0 indicate little to no relationship between 2 variables.")),
                                      mainPanel(
                                        DTOutput('hit_cor')
                                      )),
                             tabPanel('Pitching Correlations',
                                      
                                      p(HTML("Weighted Correlations of key metrics on current and next season statistics among pitchers with 40+ IP in consecutive seasons. Data from 2021-2023 seasons.")),
                                      p(HTML("A value closer to 1 or -1 indicates a strong linear relationship between 2 variables. 
                                             Values closer to 0 indicate little to no relationship between 2 variables.")),
                                      mainPanel(
                                        DTOutput('pitch_cor')
                                      ))
                             )
                             
                  )




server <- function(input, output) {
  filtered_df <- reactive({
    
    active_tab <- input$Profiles
    
    if (active_tab == 'Hitters') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      
      level <- switch(input$level,
                      "All" = c('1', '11', '14', '16'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      positions <- c(
        "C" = "C",
        "1B" = "X1B",
        "2B" = "X2B",
        "3B" = "X3B",
        "SS" = "SS",
        "OF" = "OF",
        "UT" = "UT"
      )
      
      selected_positions <- positions[input$positions]
      
      # Filter by selected positions
      filtered <- damage_df %>%
        filter((input$value_type == "PA" & PA >= input$min_value) |
                 (input$value_type == "BBE" & bbe >= input$min_value)) %>%
        filter(if_all(all_of(selected_positions), ~ . >= 1)) %>%
        # filter(if_all(all_of(input$position_group), ~ . >= 20)) %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$season) %>%
        filter_by_values(., hitting_code, input$team) %>%
        filter_by_values(., hitter_name, input$player) %>%
        select(hitter_name, hitting_code, season, PA, bbe, damage_rate, EV90th, max_EV, pull_FB_pct,
               SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
               secondary_whiff_pct, contact_vs_avg
               #adj_p_AVG, p_BABIP
               ) %>%
        mutate(contact_vs_avg = round(contact_vs_avg, 1)) %>%
        arrange(desc(damage_rate)) %>%
        rename(
          "Name" = "hitter_name",
          "Team" = "hitting_code",
          "Season" = "season",
          "BBE" = "bbe",
          "Damage/BBE (%)" = "damage_rate",
          "90th Pctile EV" = "EV90th",
          "Max EV" = 'max_EV',
          "Pulled FB (%)" = "pull_FB_pct",
          "SEAGER" = "SEAGER",
          "Selectivity (%)" = "selection_skill",
          "Hittable Pitch Take (%)" = "hittable_pitches_taken",
          "Chase (%)" = chase,
          "Z-Contact (%)" =  z_con,
          "Whiff vs. Secondaries (%)" = secondary_whiff_pct,
          "Contact Over Expected (%)" = 'contact_vs_avg',
          #"Skill-Based AVG" = 'adj_p_AVG',
          #"Skill-Based BABIP" = 'p_BABIP',
          
        )
      
      return(filtered)
      
    } else if (active_tab == 'Hitters - Percentiles') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      
      level <- switch(input$level_h_pct,
                      "All" = c('1', '11', '14', '16'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      # Filter by selected positions
      filtered <- hitter_pct %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$season_pct) %>%
        filter_by_values(., hitting_code, input$team_pct) %>%
        filter_by_values(., hitter_name, input$player_pct) %>%
        select(hitter_name, hitting_code, season, damage_pctile, EV90_pctile,
               max_pctile, pfb_pctile, SEAGER_pctile, selection_pctile, hittable_pitches_pctile, 
               chase_pctile, z_con_pctile, sec_whiff_pctile, c_vs_avg_pctile
        ) %>%
        arrange(desc(damage_pctile)) %>%
        rename(
          "Name" = "hitter_name",
          "Team" = "hitting_code",
          "Season" = "season",
          "Damage/BBE" = damage_pctile,
          "90th Pctile EV" = EV90_pctile,
          "Max EV" = max_pctile,
          "Pulled FB (%)" = pfb_pctile,
          "SEAGER" = SEAGER_pctile,
          "Selectivity (%)" = selection_pctile,
          "Hittable Pitch Take (%)" = hittable_pitches_pctile,
          "Chase (%)" = chase_pctile,
          "Z-Contact (%)" =  z_con_pctile,
          "Whiff vs. Secondaries (%)" = sec_whiff_pctile,
          "Contact Over Expected (%)" = c_vs_avg_pctile,
          #"Skill-Based AVG" = 'adj_p_AVG',
          #"Skill-Based BABIP" = 'p_BABIP',
          
        )
      
      return(filtered)
      
    } else if (active_tab == "Pitchers") {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      level <- switch(input$lg_level,
                      "All" = c('1', '11', '14'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      hand <- switch(input$pitcher_hand,
                     "Both" = c("L", "R"),
                     "LHP" = "L",
                     "RHP" = "R")
      
      filtered <- pitcher_df %>%
        filter((input$filter_type == "IP" & IP >= input$min) |
                 (input$filter_type == "TBF" & TBF >= input$min)) %>%
        filter(TBF_per_G >= input$per_game[1] & TBF_per_G <= input$per_game[2]) %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$year) %>%
        filter_by_values(., pitcher_hand, hand) %>%
        filter_by_values(., pitching_code, input$Team) %>%
        filter_by_values(., name, input$pitcher) %>%
        select(name, season, pitching_code, IP, TBF, std.ZQ, std.DMG, std.NRV, 
               fastball_velo, max_velo, fastball_vaa, rel_z, rel_x, ext, SwStr, Ball_pct, Z_Contact,
               Chase, CSW
               #, xERA9, xWHIP, xK
               ) %>%
       # mutate(xERA9 = round(xERA9, 2),
       #         xK = round(xK, 1)) %>%
        arrange(desc(std.ZQ)) %>%
        rename(
          "Name" = "name",
          "Team" = "pitching_code",
          "Season" = "season",
          "Pitch Quality" = "std.ZQ",
          "Non-BIP Skill" = "std.NRV",
          "Damage Suppression" = "std.DMG",
          "CSW (%)" = "CSW",
          "Ball (%)" = "Ball_pct",
          "SwStr (%)" = "SwStr",
          "Z-Contact (%)" = "Z_Contact",
          "Chase (%)" = "Chase",
          "Avg FA mph" = "fastball_velo",
          "Max FA mph" = 'max_velo',
          "FA VAA" = "fastball_vaa",
          "Vertical Release (ft.)" = 'rel_z',
          "Horizontal Release (ft.)" = 'rel_x',
          "Extension (ft.)" = 'ext'
          #"PQ-Based ERA" = "xERA9",
          #"PQ-Based WHIP" = 'xWHIP',
          #"PQ-Based K (%)" = 'xK'
        )
      
      return(filtered)
      
    } else if (active_tab == 'Pitchers - Percentiles') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      
      level <- switch(input$level_p_pct,
                      "All" = c('1', '11', '14', '16'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      # Filter by selected positions
      filtered <- pitcher_pct %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$season_pct_p) %>%
        filter_by_values(., pitching_code, input$team_pct_p) %>%
        filter_by_values(., name, input$player_pct_p) %>%
        select(name, season, pitching_code, PQ_pctile, DMG_pctile,
               NRV_pctile, FA_velo_pctile, FA_max_pctile, FA_vaa_pctile, rZ_pctile, rX_pctile, ext_pctile,
               SwStr_pctile, Ball_pctile, Z_con_pctile, Chase_pctile,
               CSW_pctile
        ) %>%
        arrange(desc(PQ_pctile)) %>%
        rename(
          "Name" = "name",
          "Team" = "pitching_code",
          "Season" = "season",
          "Pitch Quality" = PQ_pctile,
          "Non-BIP Skill" = NRV_pctile,
          "Damage Suppression" = DMG_pctile,
          "CSW (%)" = CSW_pctile,
          "Ball (%)" = Ball_pctile,
          "SwStr (%)" = SwStr_pctile,
          "Z-Contact (%)" = Z_con_pctile,
          "Chase (%)" = Chase_pctile,
          "Avg FA mph" = FA_velo_pctile,
          "Max FA mph" = FA_max_pctile,
          "FA VAA" = FA_vaa_pctile,
          "Vertical Release (ft.)" = rZ_pctile,
          "Horizontal Release (ft.)" = rX_pctile,
          "Extension (ft.)" = ext_pctile
        )
      
      return(filtered)
      
    } else if (active_tab == 'Individual Pitches - Percentiles') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      
      level <- switch(input$pitch_tag_level_pct,
                      "All" = c('1', '11', '14', '16'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      pitcher_hand <- switch(input$pitch_tag_hand_pct,
                             "Both" = c("L", "R"),
                             "LHP" = "L",
                             "RHP" = "R")
      
      # Filter by selected positions
      filtered <- pitch_types_pct %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$pitch_tag_year_pct) %>%
        filter_by_values(., pitcher_hand, pitcher_hand) %>%
        filter_by_values(., pitching_code, input$pitch_tag_team_pct) %>%
        filter_by_values(., name, input$pitch_tag_pitcher_pct) %>%
        filter_by_values(., pitch_tag, input$pitch_tag_pct) %>%
        select(name, pitching_code, season, pitch_tag, usage_pctile, PQ_pctile, DMG_pctile,
               NRV_pctile, velo_pctile, max_velo_pctile, vaa_pctile, haa_pctile, ivb_pctile, hb_pctile,
               SwStr_pctile, Ball_pctile, zone_pctile, Z_con_pctile, Chase_pctile,
               CSW_pctile
        ) %>%
        arrange(desc(PQ_pctile)) %>%
        rename(
          "Name" = "name",
          "Team" = "pitching_code",
          "Season" = "season",
          "Pitch Type" = pitch_tag,
          "Usage (%)" = usage_pctile,
          "Pitch Quality" = PQ_pctile,
          "Non-BIP Skill" = NRV_pctile,
          "Damage Suppression" = DMG_pctile,
          "CSW (%)" = CSW_pctile,
          "Ball (%)" = Ball_pctile,
          "SwStr (%)" = SwStr_pctile,
          "Z-Contact (%)" = Z_con_pctile,
          "Chase (%)" = Chase_pctile,
          "Velo" = velo_pctile,
          "Max Velo" = max_velo_pctile,
          "VAA" = vaa_pctile,
          "HAA" = haa_pctile,
          "IVB (in.)" = ivb_pctile,
          "HB (in.)" = hb_pctile,
          "Zone (%)" = zone_pctile
        )
      
      return(filtered)
      
    } else if (active_tab == 'League Averages - Hitting') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      
      filtered <- hitting_avg %>%
        filter_by_values(., season, input$hitting_szn) %>%
        mutate(Level = case_when(level_id == 1 ~ 'MLB',
                                 level_id == 11 ~ 'Triple-A',
                                 level_id == 14 ~ 'Low-A',
                                 level_id == 16 ~ 'Low Minors')) %>%
        select(Level, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
               SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
               contact_vs_avg) %>%
        mutate(contact_vs_avg = round(contact_vs_avg, 1)) %>%
  
        arrange(desc(damage_rate)) %>%
        rename(
          "Season" = "season",
          "BBE" = "bbe",
          "Damage/BBE (%)" = "damage_rate",
          "90th Pctile EV" = "EV90th",
          "Pulled FB (%)" = "pull_FB_pct",
          "SEAGER" = "SEAGER",
          "Selectivity (%)" = "selection_skill",
          "Hittable Pitch Take (%)" = "hittable_pitches_taken",
          "Chase (%)" = chase,
          "Z-Contact (%)" =  z_con,
          "Contact Over Expected (%)" = 'contact_vs_avg')
      
      return(filtered)
      
    } else if (active_tab == 'League Averages - Pitching') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      level <- switch(input$lgs_level,
                      "All" = c('1', '11', '14'),
                      "MLB" = '1',
                      "Triple-A" = '11',
                      "Low-A" = '14',
                      "Low Minors" = '16'
      )
      
      filtered <- pitching_avg %>%
        filter_by_values(., level_id, level) %>%
        filter_by_values(., season, input$pitching_szn) %>%
        select(season, fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
               Chase, CSW) %>%
        rename(
          "Season" = "season",
          "CSW (%)" = "CSW",
          "Ball (%)" = "Ball_pct",
          "SwStr (%)" = "SwStr",
          "Z-Contact (%)" = "Z_Contact",
          "Chase (%)" = "Chase",
          "FA mph" = "fastball_velo",
          "FA VAA" = "fastball_vaa"
        )
      
      
      return(filtered)
      
    } else if (active_tab == 'Team Hitting') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      team_hit_level <- switch(input$team_hit_level,
                                 "All" = c('1', '11', '14', '16'),
                                 "MLB" = '1',
                                 "Triple-A" = '11',
                                 "Low-A" = '14',
                                 "Low Minors" = "16"
      )
      
      
      filtered <- team_damage %>%
        filter_by_values(., level_id, team_hit_level) %>%
        filter_by_values(., season, input$team_hitting_season) %>%
        filter_by_values(., hitting_code, input$team_hitting) %>%
        select(hitting_code, season, PA, bbe, damage_rate, EV90th, pull_FB_pct,
               SEAGER, selection_skill, hittable_pitches_taken, chase, z_con,
               secondary_whiff_pct, contact_vs_avg) %>%
        mutate(contact_vs_avg = round(contact_vs_avg, 1)) %>%
        arrange(desc(damage_rate)) %>%
        rename(
          "Team" = "hitting_code",
          "Season" = "season",
          "BBE" = "bbe",
          "Damage/BBE (%)" = "damage_rate",
          "90th Pctile EV" = "EV90th",
          "Pulled FB (%)" = "pull_FB_pct",
          "SEAGER" = "SEAGER",
          "Selectivity (%)" = "selection_skill",
          "Hittable Pitch Take (%)" = "hittable_pitches_taken",
          "Chase (%)" = chase,
          "Z-Contact (%)" =  z_con,
          "Whiff vs. Secondaries (%)" = secondary_whiff_pct,
          "Contact Over Expected (%)" = 'contact_vs_avg',
          )
      
      return(filtered)
      
    } else if (active_tab == 'Team Pitching') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      team_pitch_level <- switch(input$team_pitch_level,
                            "All" = c('1', '11', '14', '16'),
                            "MLB" = '1',
                            "Triple-A" = '11',
                            "Low-A" = '14',
                            "Low Minors" = "16"
      )
      
      filtered <- team_stuff %>%
        filter_by_values(., level_id, team_pitch_level) %>%
        filter_by_values(., season, input$team_pitching_season) %>%
        filter_by_values(., pitching_code, input$team_pitching) %>%
        select(pitching_code, season, IP, std.ZQ, std.DMG, std.NRV, 
               fastball_velo, fastball_vaa, SwStr, Ball_pct, Z_Contact,
               Chase, CSW) %>%
        arrange(desc(std.ZQ)) %>%
        rename(
          "Team" = "pitching_code",
          "Season" = "season",
          "Pitch Quality" = "std.ZQ",
          "Non-BIP Skill" = "std.NRV",
          "Damage Suppression" = "std.DMG",
          "CSW (%)" = "CSW",
          "Ball (%)" = "Ball_pct",
          "SwStr (%)" = "SwStr",
          "Z-Contact (%)" = "Z_Contact",
          "Chase (%)" = "Chase",
          "FA mph" = "fastball_velo",
          "FA VAA" = "fastball_vaa"
        )
      
      
      return(filtered)
      
    } else if (active_tab == 'Individual Pitches') {
      
      filter_by_values <- function(data, column, values) {
        if ("All" %in% values) {
          return(data)
        } else {
          return(data %>% filter({{ column }} %in% values))
        }
      }
      
      pitch_level <- switch(input$pitch_tag_level,
                            "All" = c('1', '11', '14', '16'),
                            "MLB" = '1',
                            "Triple-A" = '11',
                            "Low-A" = '14',
                            "Low Minors" = "16"
      )
      
      
      pitcher_hand <- switch(input$pitch_tag_hand,
                     "Both" = c("L", "R"),
                     "LHP" = "L",
                     "RHP" = "R")
      
      filtered <- pitch_types %>%
        filter_by_values(., level_id, pitch_level) %>%
        filter_by_values(., season, input$pitch_tag_year) %>%
        filter_by_values(., pitcher_hand, pitcher_hand) %>%
        filter_by_values(., name, input$pitch_tag_pitcher) %>%
        filter_by_values(., pitching_code, input$pitch_tag_team) %>%
        filter_by_values(., pitch_group, input$pitch_group) %>%
        filter_by_values(., pitch_tag, input$pitch_tag) %>%
        filter(pitches >= input$pitch_tag_min) %>%
        select(name, pitching_code, season, pitch_tag, pitches, pct,
               std.ZQ, std.DMG, std.NRV, velo, max_velo, vaa, haa, ivb, hb, SwStr, Z_Contact, Ball_pct, Zone,
               Chase, CSW) %>%
        arrange(desc(std.ZQ)) %>%
        rename(
          "Season" = "season",
          "Name" = "name",
          "Team" = "pitching_code",
          "Pitch Type" = "pitch_tag",
          "#" = 'pitches',
          "Usage (%)" = "pct",
          "Pitch Quality" = "std.ZQ",
          "Non-BIP Skill" = "std.NRV",
          "Damage Suppression" = "std.DMG",
          "Velo" = "velo",
          "Max Velo" = 'max_velo',
          "VAA" = "vaa",
          "HAA" = "haa",
          "IVB (in.)" = "ivb",
          "HB (in.)" = "hb",
          "CSW (%)" = "CSW",
          "SwStr (%)" = "SwStr",
          "Z-Contact (%)" = "Z_Contact",
          "Chase (%)" = "Chase",
          "SwStr (%)" = "SwStr",
          "Zone (%)" = "Zone",
          "Ball (%)" = "Ball_pct"
        )
      
      
      return(filtered)
      
    }  else if (active_tab == 'PA & R/RBI Calculator') {
      
      OPS <- as.numeric(input$team_OPS)
      batting_order <- as.numeric(input$lineup_spot)
      role <- as.numeric(switch(input$role,
                     'vs RHP Only' = .735,
                     'vs LHP Only' = .265,
                     'vs Both' = 1))
      
      # Ensure batting_order has enough variability
      if(length(unique(batting_order)) == 1) {
        batting_order <- seq(1, 9)
      }
      
      lineup <- data.frame(OPS, batting_order)
      
      PA <- round(predict(PA_est, newdata = lineup, type = 'response') * as.numeric(input$player_games) * role, 0)
      Runs <- round(predict(R_est, newdata = lineup, type = 'response') * PA, 0)
      RBI <- round(predict(RBI_est, newdata = lineup, type = 'response') * PA, 0)
      
      counts <- data.frame(PA, Runs, RBI)
      
      counts <- counts[input$lineup_spot,]
      
      return(counts)
      
    } else if (active_tab == 'Hitting Correlations') {
      
      filtered <- hitting_cor %>%
        rename_all(., ~gsub("_", " ", .)) %>%
        rename_all(., ~gsub(" Pct", "(%)", .)) %>%
        mutate_all(., ~gsub("_", " ", .)) %>%
        mutate_all(., ~gsub(" Pct", "(%)", .))
        
      
      return(filtered)
      
    } else if (active_tab == 'Pitching Correlations') {
      
      filtered <- pitching_cor %>%
        arrange(desc(abs(next_yr_RA9))) %>%
        rename_all(., ~gsub("_", " ", .)) %>%
        rename_all(., ~gsub(" Pct", "(%)", .)) %>%
        mutate_all(., ~gsub("_", " ", .)) %>%
        mutate_all(., ~gsub(" Pct", "(%)", .)) 
        
      
      
      return(filtered)
    }
    
  })
  
  output$hitter_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',  # Include buttons extension for additional functionality
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',  # Define the layout of the table and buttons # old one is 'lfrtipB'
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
                ))
  })
  
  output$hitter_pcts <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',  # Include buttons extension for additional functionality
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',  # Define the layout of the table and buttons # old one is 'lfrtipB'
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$pitcher_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$pitcher_pcts <- renderDataTable({
    datatable(filtered_df(),
                     rownames = FALSE,
              extensions = 'Buttons',  # Include buttons extension for additional functionality
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',  # Define the layout of the table and buttons # old one is 'lfrtipB'
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$lg_hitting_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              options = list(
                lengthMenu = list()
                ))
    
  })
  
  output$lg_pitching_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              options = list(
                lengthMenu = list()
                ))
  })
  
  output$hit_cor <- renderDataTable({
    datatable(filtered_df(),
              options = list(
                rownames = FALSE,
                extensions = 'Buttons',  # Include buttons extension for additional functionality
                filter = "top",
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('colvis'),
                pageLength = 25,
                lengthMenu = list(),
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$pitch_cor <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              options = list(
                extensions = 'Buttons',  # Include buttons extension for additional functionality
                filter = "top",
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('colvis'),
                pageLength = 25,
                lengthMenu = list(),
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$team_hitting_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                pageLength = 30,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$team_pitching_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                pageLength = 30,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$pitch_types_table <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$pitch_types_table_pct <- renderDataTable({
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                pageLength = 25,
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T))
              ))
  })
  
  output$counting_stats <- renderDataTable({
    
    datatable(filtered_df(),
              rownames = FALSE,
              extensions = 'Buttons',
              filter = "top",
              options = list(
                dom = '<"top"B>Qrt<"bottom"lp><"clear">',
                buttons = c('csv', 'colvis'),
                columnDefs = list(list(targets = "_all", searchable = T, filterable = T)))
    )
  })
  
}

# Run the application 
shinyApp(ui = ui, server = server)

