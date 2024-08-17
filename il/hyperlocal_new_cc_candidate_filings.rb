# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - IL CC Candidate Filings
# Autor: Alberto Egurrola
# Date: April 2022
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::il::hyperlocal_new_cc_candidate_filings" --mode='process_1'

require_relative '../../../../lib/mini_loki_c/hle_clean_common.rb'

# - - - - - - - - - - - - -
# DATABASE DEFINITION
# - - - - - - - - - - - - -
# Define source and destination host/db
# Add here any other variables/constants needed
# - - - - - - - - - - - - -
SOURCE_HOST      = 'db01'
SOURCE_DB        = 'voters_2016'
DESTINATION_HOST = 'db01'
DESTINATION_DB   = 'voters_2016'
SLACK_ID         = 'U0SS1D1K6'
TOOLNAME         = __FILE__.to_s.gsub(/.*\/clean\//i, 'clean/').gsub(/\.rb$/i, '').gsub('/', '::')
MSG_TITLE        = "*[HLE Cleaning] #{TOOLNAME}*"
TOOLS            = MiniLokiC::HLECleanCommon::Tools
DB               = MiniLokiC::HLECleanCommon::DB
$options = {
  'task' => 'all'
}
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  $options.merge!(opts.clone)

  start_time = Time.now
  TOOLS.process_message(start_time, 'script', 'main process', SLACK_ID, MSG_TITLE, 'start', $options)

  case $options['mode']
  when 'create_tables'
    create_tables
  when 'process_1'
    process_1
  when 'process_2'
    process_2
  when 'run_all'
    create_tables
    process_1 # names, locations
    process_2 # match locations
  else
  	nil
  end

  TOOLS.process_message(Time.now, 'script', 'main process', SLACK_ID, MSG_TITLE, 'end', $options, start_time)
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
def process_1
  method_desc = 'clean names and locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - - 
  processed_records = 0

  source_table_1        = "hyperlocal_new_cc_candidate_filings"
  dest_table_cleaned    = "hyperlocal_new_cc_candidate_filings_cleaned"
  dest_table_names      = "hyperlocal_new_cc_candidate_filings_names_unique"
  dest_table_locations  = "hyperlocal_new_cc_candidate_filings_locations_unique"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      #{source_table_1}.first_name,
      #{source_table_1}.last_name,
      #{source_table_1}.city,
      #{source_table_1}.state,
      #{source_table_1}.zip
    FROM
      #{source_table_1}
      #{$options['new_records_only'] ? "
        LEFT JOIN #{dest_table_cleaned}
          ON #{dest_table_cleaned}.raw_id = #{source_table_1}.id
      " : ""}
    WHERE
      #{$options['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ""}
      #{source_table_1}.last_name is not null
      and #{source_table_1}.last_name != ''
      and #{source_table_1}.first_name is not null
      and #{source_table_1}.first_name != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['entity_name']} - #{record['entity_type']}"

    name = {}
    location = {}

    # - - - - - - - - - - - - - - - - - - -

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['first_name'] = record['first_name'].strip
    name['last_name'] = record['last_name'].strip

    if $options['task'] == 'all' || $options['task'] == 'name'
      name['full_name_cleaned'], name['name_type'] = TOOLS.clean_name_1("#{name['first_name'].gsub(/\s*"[a-zA-Z]+"\s*/i, ' ').strip} #{name['last_name']}".gsub!(/\s+/,' '), reverse = false, name_type = nil, determiner)
      if name['name_type'] == 'Organization'
        name['full_name_cleaned'] = TOOLS.clean_org_name_1(name['full_name_cleaned'])
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['city'] = "#{record['city']}"
    location['state'] = "#{record['state']}"
    location['zip'] = "#{record['zip']}"

    if $options['task'] == 'all' || $options['task'] == 'location'
      city_cleaned = TOOLS.clean_city(location['city'])
      location['city_cleaned'] = city_cleaned.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(city_cleaned, location['state'], 1) : city_cleaned
      location['city_cleaned'] = location['city_cleaned'] == nil ? city_cleaned : location['city_cleaned']
    end

    # - - - - - - - - - - - - - - - - - - -

    if $options['debug']
      puts name
      puts '- ' * 10
      puts location
      puts '- ' * 10
    else
      # name
      name_id = DB.run_task($options, 'name', DESTINATION_HOST, DESTINATION_DB, dest_table_names, name, { 'first_name' => name['first_name'], 'last_name' => name['last_name']})

      # location
      location_id = DB.run_task($options, 'location', DESTINATION_HOST, DESTINATION_DB, dest_table_locations, location, {'city' => location['city'], 'state' => location['state'], 'zip' => location['zip']})

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "location_id: #{location_id}"
      puts "- " * 10

      # global clean record
      unless name_id && location_id
        puts "[skip] -> missing [name_id|location_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'location_id' => location_id,
        }
        clean_id = DB.run_task($options, 'main clean', DESTINATION_HOST, DESTINATION_DB, dest_table_cleaned, clean_data, { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] })
        processed_records += 1 if clean_id
      end
    end

    puts "= " * 20
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

def process_2
  method_desc = 'match locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0
  source_table_1 = "hyperlocal_new_cc_candidate_filings_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      #{$options['new_records_only'] ? "(was_matched is null or was_matched = 0) and" : ''}
      city_cleaned is not null and city != ''
      and state is not null and state != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['city_cleaned']}, #{record['state']}... "
    matched_data = TOOLS.match_city(record['city_cleaned'], record['state'])

    if matched_data
      puts "City matched: #{matched_data['place_name']} - #{matched_data['pl_production_org_id']}"
      DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] , 'was_matched' => 1}, { 'id' => record['raw_id'] })
      processed_records += 1
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# TABLES DEFINITION
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define the new clean dataset tables
# use one hash per table with neccesary fields:
#  * table_name - new table name (start them with "hle_clean_")
#  * columns    - column definition
#  * indexes    - set of indexes and keys (for a unique key)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def define_tables
  tables = [
    {
      'table_name' => 'hyperlocal_new_cc_candidate_filings_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        location_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, location_id)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'hyperlocal_new_cc_candidate_filings_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        first_name varchar(255) not null,
        last_name varchar(255) not null,
        full_name_cleaned varchar(255),
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (first_name, last_name)'
    },
    {
      'table_name' => 'hyperlocal_new_cc_candidate_filings_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        raw_field varchar(255),
        city varchar(255) not null,
        city_cleaned varchar(100),
        state varchar(50) not null,
        zip varchar(20) not null,
        usa_adcp_matching_id int unsigned,
        was_matched tinyint(1) not null default 0,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state, zip)',
      'charset' => 'utf8mb4',
    },
  ]

  return tables
end
