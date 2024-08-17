# frozen_string_literal: true

# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - IL Parole Crime Types
# Autor: Alberto Egurrola
# Date: March 2023
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::il::il_parole_crime_types" --mode='process_1'

require 'nokogiri'
require 'scylla'
require_relative '../../../../lib/mini_loki_c/hle_clean_common'

# - - - - - - - - - - - - -
# DATABASE DEFINITION
# - - - - - - - - - - - - -
# Define source and destination host/db
# Add here any other variables/constants needed
# - - - - - - - - - - - - -
SOURCE_HOST      = 'db15'
SOURCE_DB        = 'hle_data'
DESTINATION_HOST = 'db15'
DESTINATION_DB   = 'hle_data'
SLACK_ID         = 'U0SS1D1K6'
TOOLNAME         = __FILE__.to_s.gsub(%r{.*/clean/}i, 'clean/').gsub(/\.rb$/i, '').gsub('/', '::')
MSG_TITLE        = "*[HLE Cleaning] #{TOOLNAME}*"
TOOLS            = MiniLokiC::HLECleanCommon::Tools
DB               = MiniLokiC::HLECleanCommon::DB
OPTIONS          = MiniLokiC::HLECleanCommon::Options.new
DEV_NAME         = 'Alberto Egurrola'
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  OPTIONS.merge!(opts.clone)

  start_time = Time.now
  TOOLS.process_message(start_time, 'script', 'main process', SLACK_ID, MSG_TITLE, 'start', OPTIONS)

  begin
    case OPTIONS['mode']
    when 'create_tables'
      create_tables
    when 'process_1'
      process1
    when 'run_all'
      create_tables
      process1 # clean crimes
    end
  rescue StandardError => e
    puts e.backtrace
    TOOLS.slack_message(SLACK_ID, "#{MSG_TITLE}\n\nError!:\n#{e.message}\n\n#{e.backtrace}")
  end
  TOOLS.process_message(Time.now, 'script', 'main process', SLACK_ID, MSG_TITLE, 'end', OPTIONS, start_time)
end

def create_tables
  # create new "hle_clean" tables
  tables = define_tables
  DB.create_tables(DESTINATION_HOST, DESTINATION_DB, tables)
end

# - - - - - - - - - - - - -
# INSERT/CLEAN PROCESS FUNCTIONS
# - - - - - - - - - - - - -
# Define any insert/clean process below
# Use different functions for each process,
# for better mainteinance
# - - - - - - - - - - - - -
def process1
  method_desc = 'clean crime types'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', OPTIONS)
  # - - - - - - - - - - - -
  processed_records = 0
  source_table        = 'il_parole_population_date_scrape'
  dest_table_cleaned  = 'il_parole_population_date_scrape_crime_types'

  query = <<HERE
    SELECT
      #{source_table}.id as raw_id,
      '#{source_table}' as raw_source,
      #{source_table}.holding_offense_category,
      #{source_table}.offense_type
    FROM
      #{source_table}
      #{if OPTIONS['new_records_only']
          "LEFT JOIN #{dest_table_cleaned}
            ON #{dest_table_cleaned}.holding_offense_category = #{source_table}.holding_offense_category"
        end
      }
      #{OPTIONS['join'] ? " #{OPTIONS['join']}" : ''}
    WHERE
      #{OPTIONS['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ''}
      #{source_table}.holding_offense_category is not null
      and #{source_table}.holding_offense_category != ''
      #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : ''}
      #{OPTIONS['group_by'] ? " GROUP BY #{OPTIONS['group_by']}" : ' GROUP BY holding_offense_category'}
      #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do |record|
    c += 1
    puts "[#{c}/#{results.size}] processing: #{record['holding_offense_category']}"

    crime_type = {}

    # - - - - - - - - - - - - - - - - - - -

    crime_type['raw_id'] = record['raw_id']
    crime_type['raw_source'] = record['raw_source']
    crime_type['holding_offense_category'] = record['holding_offense_category'].to_s
    crime_type['created_by'] = DEV_NAME

    if OPTIONS['task'] == 'all' || OPTIONS['task'] == 'crime'
      crime_type['holding_offense_category_clean'], crime_type['crime_type_clean'] = clean_crime(crime_type['holding_offense_category'])
    end

    # - - - - - - - - - - - - - - - - - - -

    if OPTIONS['debug']
      puts crime_type
      puts '- ' * 10
    else
      crime_type_id = DB.run_task(
        OPTIONS,
        'crime_type',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_cleaned,
        crime_type,
        {
          'holding_offense_category' => crime_type['holding_offense_category']
        }
      )
      processed_records += 1 if crime_type_id
    end
    puts '= ' * 20
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', OPTIONS,
                        start_time, processed_records)
end

private

def clean_crime(crime)
  clean_crimes = {
    'Alcohol Crimes' => {
      'holding_offense_category_clean' => 'Alcohol Crimes',
      'clean_crime_type' => 'a crime involving alcohol'
    },
    'Attempt, Conspiracy and Aiding' => {
      'holding_offense_category_clean' => 'Attempt, Conspiracy and Aiding',
      'clean_crime_type' => 'a crime involving conspiracy or aiding and abetting'
    },
    'Crimes against Children' => {
      'holding_offense_category_clean' => 'Crimes against Children',
      'clean_crime_type' => 'a crime against one or more children'
    },
    'Crimes Against Justice' => {
      'holding_offense_category_clean' => 'Crimes Against Justice',
      'clean_crime_type' => 'a crime against justice'
    },
    'Crimes against the Government' => {
      'holding_offense_category_clean' => 'Crimes against the Government',
      'clean_crime_type' => 'a crime against the government'
    },
    'Crimes against the Person' => {
      'holding_offense_category_clean' => 'Crimes against the Person',
      'clean_crime_type' => 'a crime against a person'
    },
    'Cyber Crimes' => {
      'holding_offense_category_clean' => 'Cyber Crimes',
      'clean_crime_type' => 'a cyber crime'
    },
    'Drug Charges' => {
      'holding_offense_category_clean' => 'Drug Charges',
      'clean_crime_type' => 'a crime involving drugs'
    },
    'Fraud and Financial Crimes' => {
      'holding_offense_category_clean' => 'Fraud and Financial Crimes',
      'clean_crime_type' => 'a financial crime or fraud'
    },
    'Homicide' => {
      'holding_offense_category_clean' => 'Homicide',
      'clean_crime_type' => 'homicide'
    },
    'Property Crimes' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Public Safety Violations' => {
      'holding_offense_category_clean' => 'Public Safety Violations',
      'clean_crime_type' => 'a public safety violation'
    },
    'Sex Crimes' => {
      'holding_offense_category_clean' => 'Sex Crimes',
      'clean_crime_type' => 'a sex crime'
    },
    'Armed Robbery' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Armed Violence' => {
      'holding_offense_category_clean' => 'Weapons',
      'clean_crime_type' => 'a crime involving one or more weapons'
    },
    'Arson' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Assault-Battery-Force-Harm' => {
      'holding_offense_category_clean' => 'Crimes against the Person',
      'clean_crime_type' => 'a crime against a person'
    },
    'Bail Bond Viol' => {
      'holding_offense_category_clean' => 'Fraud and Financial Crimes',
      'clean_crime_type' => 'a financial crime or fraud'
    },
    'Burglary' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Cannabis' => {
      'holding_offense_category_clean' => 'Drug Charges',
      'clean_crime_type' => 'a crime involving drugs'
    },
    'Control Substance Viol' => {
      'holding_offense_category_clean' => 'Drug Charges',
      'clean_crime_type' => 'a crime involving drugs'
    },
    'Crim Damage to Prop' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Disorderly Conduct' => {
      'holding_offense_category_clean' => 'Public Safety Violations',
      'clean_crime_type' => 'a public safety violation'
    },
    'DUI-Liguor' => {
      'holding_offense_category_clean' => 'Alcohol Crimes',
      'clean_crime_type' => 'a crime involving alcohol'
    },
    'Escape or Aid' => {
      'holding_offense_category_clean' => 'Crimes Against Justice',
      'clean_crime_type' => 'a crime against justice'
    },
    'Forgery-Deception-Fraud' => {
      'holding_offense_category_clean' => 'Fraud and Financial Crimes',
      'clean_crime_type' => 'a financial crime or fraud'
    },
    'Govt-Bribery-Business' => {
      'holding_offense_category_clean' => 'Fraud and Financial Crimes',
      'clean_crime_type' => 'a financial crime or fraud'
    },
    'Habitual Criminal' => {
      'holding_offense_category_clean' => 'Multiple Crimes',
      'clean_crime_type' => 'multiple crimes'
    },
    'Home-Veh Invasion' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Inchoate' => {
      'holding_offense_category_clean' => 'Attempt, Conspiracy and Aiding',
      'clean_crime_type' => 'a crime involving conspiracy or aiding and abetting'
    },
    'Kidnapping' => {
      'holding_offense_category_clean' => 'Crimes against the Person',
      'clean_crime_type' => 'a crime against a person'
    },
    'Motor Veh Offenses' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Motor Veh Theft' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Other Sex Offenses' => {
      'holding_offense_category_clean' => 'Sex Crimes',
      'clean_crime_type' => 'a sex crime'
    },
    'Poss Hypos' => {
      'holding_offense_category_clean' => 'Drug Charges',
      'clean_crime_type' => 'a crime involving drugs'
    },
    'Rape-Before 7-84' => {
      'holding_offense_category_clean' => 'Sex Crimes',
      'clean_crime_type' => 'a sex crime'
    },
    'Residential Burglary' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Retail Theft' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Robbery' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'SDP' => {
      'holding_offense_category_clean' => 'Sex Crimes',
      'clean_crime_type' => 'a sex crime'
    },
    'Sex Assault After 7-84' => {
      'holding_offense_category_clean' => 'Sex Crimes',
      'clean_crime_type' => 'a sex crime'
    },
    'Theft' => {
      'holding_offense_category_clean' => 'Property Crimes',
      'clean_crime_type' => 'a property crime'
    },
    'Weapons' => {
      'holding_offense_category_clean' => 'Weapons',
      'clean_crime_type' => 'a crime involving one or more weapons'
    }
  }

  return nil, nil unless clean_crimes.key?(crime)

  [clean_crimes[crime]['holding_offense_category_clean'], clean_crimes[crime]['clean_crime_type']]
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# TABLES DEFINITION
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define the new clean dataset tables
# use one hash per table with neccesary fields:
#  * table_name - new table name
#  * columns    - column definition
#  * indexes    - set of indexes and keys (for a unique key)
#  * charset    - default charset for new table (utf8mb4)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def define_tables
  [
    {
      'table_name' => 'il_parole_population_date_scrape_crime_types',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        holding_offense_category varchar(255) not null,
        holding_offense_category_clean varchar(255),
        crime_type_clean varchar(255),
        fixed_manually tinyint(1) not null default 0,
        created_by varchar(150),
      ",
      'indexes' => 'unique key (holding_offense_category)'
    }
  ]
end
