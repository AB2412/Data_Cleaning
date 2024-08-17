# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Illinois Sex Offenders
# Autor: Alberto Egurrola
# Date: April 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::il::il_sex_offenders" --mode='process_1'

require_relative '../../../../lib/mini_loki_c/hle_clean_common.rb'

# - - - - - - - - - - - - -
# DATABASE DEFINITION
# - - - - - - - - - - - - -
# Define source and destination host/db
# Add here any other variables/constants needed
# - - - - - - - - - - - - -
SOURCE_HOST      = 'db01'
SOURCE_DB        = 'usa_raw'
DESTINATION_HOST = 'db01'
DESTINATION_DB   = 'usa_raw'
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
    insert_process_1
  when 'process_2'
    insert_process_2
  when 'run_all'
    create_tables
    insert_process_1 # names, locations
    insert_process_2 # match locations
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
def insert_process_1
  method_desc = 'clean names and locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - - 
  processed_records = 0

  source_table_1 = "illinois_sex_offenders"
  dest_table_1 = "illinois_sex_offenders_names_unique"
  dest_table_2 = "illinois_sex_offenders_locations_unique"
  dest_table_3 = "illinois_sex_offenders_cleaned"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      #{source_table_1}.full_name,
      #{source_table_1}.first_name,
      #{source_table_1}.middle_name,
      #{source_table_1}.last_name,
      #{source_table_1}.sex,
      #{source_table_1}.city,
      #{source_table_1}.state
    FROM
      #{source_table_1}
    LEFT JOIN #{dest_table_3}
      ON #{dest_table_3}.raw_id = #{source_table_1}.id
    WHERE
      #{$options['new_records_only'] ? " #{dest_table_3}.id is null and " : ""}
      #{source_table_1}.full_name is not null and #{source_table_1}.full_name != ''
      #{$options['where'] ? " AND #{$options['where']}" : ''}
      #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['full_name']}... "

    name = {}
    location = {}

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['full_name'] = record['full_name']
    if record['first_name'] == '' && record['middle_name'] != ''
      if record['middle_name'] =~ /^(\S+)\s+(\S.*)/i
        name['first_name'] = $1
        name['middle_name'] = $2
      elsif record['middle_name'] =~ /^(\S+)/i
        name['first_name'] = $1
        name['middle_name'] = ''
      else # shouldn't happen
        name['first_name'] = ''
        name['middle_name'] = ''
      end
    else
      name['first_name'] = record['first_name']
      name['middle_name'] = record['middle_name']
    end
    name['first_name'] = TOOLS.titleize(name['first_name'])
    name['first_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['middle_name'] = TOOLS.titleize(name['middle_name'])
    name['middle_name'].gsub!(/(^|\s+)([a-zA-Z])\.?(\s+|\s*$)/i) {"#{$1}#{$2.upcase}.#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)([IV]{2,})(\s+|$)/i) {"#{$1}#{$2.upcase}#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)Jr$/i, '\1Jr.')
    name['middle_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['last_name'] = TOOLS.titleize(record['last_name'])
    name['last_name'].gsub!(/-\s*([a-zA-Z])/i) {"-#{$1.upcase}"}
    if $options['task'] == 'all' || $options['task'] == 'name'
      name['full_name_cleaned'], name['name_type'] = TOOLS.clean_name_1(name['full_name'])
    end

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['city'] = record['city']
    location['state'] = record['state']
    location['city_cleaned'] = TOOLS.clean_city(record['city'])

    if $options['debug']
      puts name
      puts '- ' * 10
      puts location
    else
      # business names
      name_id = DB.run_task(
        $options,
        'name',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_1,
        name,
        { 'full_name' => name['full_name'] }
      )

      # locations
      location_id = DB.run_task(
        $options,
        'location',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_2,
        location,
        {'city' => location['city'], 'state' => location['state']}
      )

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
        clean_id = DB.run_task(
          $options,
          'main clean',
          DESTINATION_HOST,
          DESTINATION_DB,
          dest_table_3,
          clean_data,
          { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] }
        )
        processed_records += 1 if clean_id
      end
    end

    puts "= " * 20
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

def insert_process_2
  method_desc = 'match locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - - 
  processed_records = 0
  source_table_1 = "illinois_sex_offenders_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      #{$options['new_records_only'] ? "(usa_adcp_matching_id is null or usa_adcp_matching_id = 0) and" : ''}
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
      DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] }, { 'id' => record['raw_id'] })
      processed_records += 1
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

private

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
      'table_name' => 'illinois_sex_offenders_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        location_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, location_id)'
    },
    {
      'table_name' => 'illinois_sex_offenders_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        full_name varchar(255) not null,
        full_name_cleaned varchar(255) not null,
        first_name varchar(255) not null,
        middle_name varchar(255) not null,
        last_name varchar(255) not null,
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (full_name)'
    },
    {
      'table_name' => 'illinois_sex_offenders_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        city varchar(100) not null,
        city_cleaned varchar(100),
        state varchar(50) not null,
        usa_adcp_matching_id int unsigned,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state)'
    },
  ]

  return tables
end
