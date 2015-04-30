#!/usr/bin/env ruby
require 'soda'
require 'certified'

raise 'SocrataAppTokenEnvironmentVariableUnset' if ENV['SOCRATA_APP_TOKEN'].nil?

# Configure the dataset ID and initialize SODA client
dataset_resource_id = "dgzq-9mgi" # URL: https://opendata.demo.socrata.com/d/dgzq-9mgi
soda_client = SODA::Client.new({
  domain: "opendata.demo.socrata.com",
  app_token: ENV['SOCRATA_APP_TOKEN']
})

LATEST_YEAR = 2012

# mappings from code => value (from data dictionary which is a PDF)
DATA_DICTIONARY = {
  # "museum_type" => {
  #   "ART" => "Art Museums",
  #   "BOT" => "Arboretums, Botanical Gardens, & Nature Centers",
  #   "CMU" => "Children's Museums",
  #   "GMU" => "Uncategorized or General Museums",
  #   "HSC" => "Historical Societies, Historic Preservation",
  #   "HST" => "History Museums",
  #   "NAT" => "Natural History & Natural Science Museums",
  #   "SCI" => "Science & Technology Museums & Planetariums",
  #   "ZAW" => "Zoos, Aquariums, & Wildlife Conservation",
  # },
  # "nces_locale_code" => {
  #   "1" => "City",
  #   "2" => "Suburb",
  #   "3" => "Town",
  #   "4" => "Rural"
  # },
  # "aam_museum_region" => {
  #   "1" => "New England",
  #   "2" => "Mid-Atlantic",
  #   "3" => "Southeastern",
  #   "4" => "Midwest",
  #   "5" => "Mount Plains",
  #   "6" => "Western"
  # },
  # "micropolitan_area_flag" => {
  #   "0" => "Not in a micropolitan statstical area (µSA)",
  #   "1" => "In a micropolitan statistical area (µSA)"
  # },
  # "irs_990_flag" => {
  #   "0" => "IRS form 990 data source not used",
  #   "1" => "IRS form 990 data source used"
  # },
  # "imls_admin_data_source_flag" => {
  #   "0" => "IMLS administrative data source not used",
  #   "1" => "IMLS administrative data source used"
  # },
  # "third_party_source_flag" => {
  #   "0" => "Third party (Factual) source not used",
  #   "1" => "Third party (Factual) source used"
  # },
  # "private_grant_foundation_data_source_flag" => {
  #   "0" => "Private grant foundation data source not used",
  #   "1" => "Private grant foundation data source used"
  # }
}

SCHEDULER.every '1m', first_in: 0 do |job|

  #### COUNT BY YEAR ####
  count_by_year_response = soda_client.get(dataset_resource_id, {
    "$group" => "year_date",
    "$select" => "year_date, COUNT(*) AS n",
  })
  count_by_year = {}
  count_by_year_response.each do |item|
    count_by_year[item.year_date] = {:label => item.year_date, :value => item.n}
  end
  send_event('count_by_year', { items: count_by_year.values })


  #### TOTAL SURVEYS ####
  total_surveys_response = soda_client.get(dataset_resource_id, {
    "$select" => "count(*)"
  })
  total_surveys = total_surveys_response.first["count"].to_i
  send_event('total_surveys', { current:  total_surveys})

  #### TOTAL SURVEYS BY YEAR ####
  years_of_data_response = soda_client.get(dataset_resource_id, {
    "$select" => "year_date,count(*)",
    "$group" => "year_date",
    "$order" => "year_date asc"
  })
  years_of_data = years_of_data_response.count
  send_event('years_of_data', { current:  years_of_data})


  #### TOTAL MALE ####
  total_2012_male_response = soda_client.get(dataset_resource_id, {
    "$where" => "year_num = 2012 AND q1 = 'Hombre'",
    "$select" => "count(*)"
  })
  total_2012_male = total_2012_male_response.first["count"]
  send_event('total_2012_male', { current:  total_2012_male})

  total_2012_response = soda_client.get(dataset_resource_id, {
    "$where" => "year_num = 2012",
    "$select" => "count(*)"
  })
  total_2012 = total_2012_response.first["count"]

  #### PERCENT MALE ####
  percent_2012_male = ((total_2012_male.to_f/total_2012.to_f)*100).to_i
  send_event('percent_2012_male', { value:  percent_2012_male})

  #### COUNT BY TOP CONCERNS ####
  count_by_concern_response = soda_client.get(dataset_resource_id, {
    "$where" => "year_num = 2012 AND a4 != 'NA'",
    "$group" => "a4",
    "$select" => "a4, COUNT(*) AS n",
    "$order" => "n desc",
    "$limit" => 7
  })
  count_by_concern = {}
  count_by_concern_response.each do |item|
    count_by_concern[item.a4] = {:label => item.a4, :value => item.n}
  end
  send_event('count_by_concern', { items: count_by_concern.values })
  puts count_by_concern.inspect
  puts "x"
end