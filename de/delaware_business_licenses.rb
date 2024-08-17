# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Delaware Business Licenses
# Autor: Alberto Egurrola
# Date: April 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="delaware_business_licenses::delaware_business_licenses" --mode='process_1' --debug

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
$options = {
  'task' => 'all'
}
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  $options.merge!(opts.clone)

  case $options['mode']
  when 'create_tables'
    create_tables
  when 'process_1'
    insert_process_1
  when 'process_2'
    insert_process_2
  when 'run_all'
    create_tables
    insert_process_1 # names, activities, locations
    insert_process_2 # match locations
  else
  	nil
  end
end

def create_tables
  # create new "hle_clean" tables
  tables = define_tables
  MiniLokiC::HLECleanCommon::DB.create_tables(DESTINATION_HOST, DESTINATION_DB, tables)
end

# - - - - - - - - - - - - -
# INSERT/CLEAN PROCESS FUNCTIONS
# - - - - - - - - - - - - -
# Define any insert/clean process below
# Use different functions for each process,
# for better mainteinance
# - - - - - - - - - - - - -
def insert_process_1
  puts "---> insert_process_1: start"
  source_table_1 = "delaware_business_licenses"
  dest_table_1 = "delaware_business_licenses_names_unique"
  dest_table_2 = "delaware_business_licenses_activities_unique"
  dest_table_3 = "delaware_business_licenses_locations_unique"
  dest_table_4 = "delaware_business_licenses_cleaned"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      busines_name as business_name,
      business_activity,
      location
    FROM
      #{source_table_1}
    LEFT JOIN #{dest_table_4}
      ON #{dest_table_4}.raw_id = #{source_table_1}.id
    WHERE
      #{$options['new_records_only'] ? " #{dest_table_4}.id is null and " : ""}
      busines_name is not null and busines_name != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = MiniLokiC::HLECleanCommon::DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['business_name']}... "

    name = {}
    activity = {}
    location = {}

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['business_name'] = record['business_name']
    name['business_name'] = '' if name['business_name'] =~ %r{^\s*\\"?\s*$}
    if $options['task'] == 'all' || $options['task'] == 'name'
      name['business_name_cleaned'], name['name_type'] = MiniLokiC::HLECleanCommon::Tools.clean_name_1(
        name['business_name'],
        reverse: false,
        name_type: nil,
        determiner: determiner)
      name['business_name_cleaned'] = MiniLokiC::HLECleanCommon::Tools.clean_org_name_1(name['business_name_cleaned'])
    end

    activity['raw_id'] = record['raw_id']
    activity['raw_source'] = record['raw_source']
    activity['business_activity'] = record['business_activity']
    activity['business_activity_cleaned'] = MiniLokiC::HLECleanCommon::Tools.titleize(activity['business_activity'])

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['location'] = record['location']
    location['address'], location['city'], location['state'], location['zip'] = clean_location(location['location'])

    if $options['debug']
      if $options['task'] == 'all'
        puts name
        puts '- ' * 10
        puts activity
        puts '- ' * 10
        puts location
      elsif $options['task'] == 'name'
        puts name
        puts '- ' * 10
      elsif $options['task'] == 'activity'
        puts activity
        puts '- ' * 10
      elsif $options['task'] == 'location'
        puts location
        puts '- ' * 10
      end
    else
      # business names
      name_id = run_task('name', DESTINATION_HOST, DESTINATION_DB, dest_table_1, name, { 'business_name' => name['business_name'] })

      # business activities
      activity_id = run_task('activity', DESTINATION_HOST, DESTINATION_DB, dest_table_2, activity, { 'business_activity' => activity['business_activity'] })

      # locations
      location_id = run_task('location', DESTINATION_HOST, DESTINATION_DB, dest_table_3, location, {'location' => location['location']})

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "activity_id: #{activity_id}"
      puts "location_id: #{location_id}"
      puts "- " * 10

      # global clean record
      unless name_id && activity_id && location_id
        puts "[skip] -> missing [name_id|activity_id|location_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'activity_id' => activity_id,
          'location_id' => location_id,
        }
        clean_id = run_task('main clean', DESTINATION_HOST, DESTINATION_DB, dest_table_4, clean_data, { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] })
      end
    end

    puts "= " * 20
  end
  puts "---> insert_process_1: finish"
end

def run_task(task, host, db, table, data, key)
  rec_id = MiniLokiC::HLECleanCommon::DB.get_id_query(host, db, table, key)
  if $options['task'].downcase == 'all' || $options['task'].downcase == task.downcase || task == 'main clean'
    if rec_id && ($options['force'] || $options['update'])
      puts "#{task} -- force update: #{rec_id}"
      MiniLokiC::HLECleanCommon::DB.update_query(host, db, table, data, { 'id' => rec_id })
    elsif rec_id.nil?
      puts "#{task} -- insert"
      MiniLokiC::HLECleanCommon::DB.insert_query(host, db, table, data)
      rec_id = MiniLokiC::HLECleanCommon::DB.get_id_query(host, db, table, key)
    else
      puts "#{task} record found [no update]: #{rec_id}  -  #{key.map{|k,v| "#{k} => #{v}"}.join(', ')}"
    end
  end
  return rec_id
end

def insert_process_2
  puts "---> insert_process_2: start"
  source_table_1 = "delaware_business_licenses_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      location,
      city,
      state
    FROM
      #{source_table_1}
    WHERE
      city is not null and city != ''
      and state is not null and state != ''
    #{$options['new_records_only'] ? " AND (matched_id is null or matched_id = 0)" : ''}
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = MiniLokiC::HLECleanCommon::DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['city']}, #{record['state']}... "
    matched_data =MiniLokiC::HLECleanCommon::Tools.match_city(record['city'], record['state'])

    if matched_data
      puts "City matched: #{matched_data['place_name']} - #{matched_data['pl_production_org_id']}"
      MiniLokiC::HLECleanCommon::DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, matched_data, { 'id' => record['raw_id']})
    else
      puts "Not found: #{record['city']}, #{record['state']}"
    end
  end
  puts "---> insert_process_2: finish"
end

def clean_location(location)
  address = ""
  city = ""
  state = ""
  zip = ""
  if location && location=~/(.+),\s+([a-zA-Z]{2})\s+(\d+)\s*$/i
    city, state, zip = $1, $2.upcase, $3
    if city =~ /(.+)\s*,\s*([^\,]+)\s*$/
      address = MiniLokiC::HLECleanCommon::Tools.titleize($1)
      city = $2
    end
    city = MiniLokiC::HLECleanCommon::Tools.clean_city(city)
  end

  address.gsub!(/\s{2,}/, ' ')
  city.gsub!(/\s{2,}/, ' ')
  state.gsub!(/\s{2,}/, ' ')
  zip.gsub!(/\s{2,}/, ' ')

  return address.strip, city.strip, state.strip, zip.strip
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
      'table_name' => 'delaware_business_licenses_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        activity_id int unsigned not null,
        location_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, activity_id, location_id)'
    },
    {
      'table_name' => 'delaware_business_licenses_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        business_name varchar(255) not null,
        business_name_cleaned varchar(255),
        business_name_cleaned_manual varchar(255),
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (business_name)'
    },
    {
      'table_name' => 'delaware_business_licenses_activities_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        business_activity varchar(255) not null,
        business_activity_cleaned varchar(255),
        business_activity_cleaned_manual varchar(255),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (business_activity)'
    },
    {
      'table_name' => 'delaware_business_licenses_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        location varchar(255) not null,
        location_cleaned varchar(255),
        location_cleaned_manual varchar(255),
        fixed_manually tinyint(1) not null default 0,
        address varchar(255),
        city varchar(100),
        state varchar(50),
        zip varchar(20),
        matched_id int unsigned,
        place_name varchar(255),
        county_name varchar(255),
        pl_production_org_id bigint(20),
        pl_production_org_name varchar(255),
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (location)'
    },
  ]

  return tables
end
