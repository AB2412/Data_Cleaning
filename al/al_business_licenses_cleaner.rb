# Creator:      Sergii Butrymenko
# Dataset Name: Alabama Business Licenses
# Task #:       11
# Created:      February 2021

# ruby mlc.rb --tool="clean::al::al_business_licenses_cleaner" --mode='names'
# ruby mlc.rb --tool="clean::al::al_business_licenses_cleaner" --mode='names' --where="last_scrape_date>='2019-01-01'"
# ruby mlc.rb --tool="clean::al::al_business_licenses_cleaner" --mode='cities'

CL_ENTITY_NAME_TBL_INFO = {
    source_table_name: 'alabama_business_licenses',
    host: DB13,
    stage_db: 'usa_raw',
    raw_name_column: 'entity_name',
    scrape_date_column: 'last_scrape_date',
    clean_table_name: 'alabama_business_licenses__names_clean',
    clean_name_column: 'entity_name_clean',
    # clean_name_type_column: 'company_name_type',
    clean_scrape_date_column: 'scrape_date',
    # person_name_reverse: true,
    # person_name_with_comma: true,
}

CL_AGENT_NAME_TBL_INFO = {
    source_table_name: 'alabama_business_licenses',
    host: DB13,
    stage_db: 'usa_raw',
    raw_name_column: 'registered_agent_name',
    scrape_date_column: 'last_scrape_date',
    clean_table_name: 'alabama_business_licenses__agent_names_clean',
    clean_name_column: 'agent_name_clean',
    clean_name_type_column: 'agent_name_type',
    clean_scrape_date_column: 'scrape_date',
    person_name_reverse: true,
    person_name_with_comma: true,
}

CL_PRINC_CITY_TBL_INFO = {
    table_name: 'alabama_business_licenses',
    host: DB13,
    stage_db: 'usa_raw',
    raw_city_state_zip_column: 'principal_city_state_zip',
    raw_city_column: 'principal_city',
    raw_state_column: 'principal_state',
    raw_zip_column: 'principal_zip',

    clean_city_column: 'principal_city_clean',
    city_org_id_column: 'principal_city_org_id',
    raw_address1_column: 'principal_address',
    # raw_address2_column: '',
    clean_address_column: 'principal_address_clean',
}

CL_OFF_CITY_TBL_INFO = {
    table_name: 'alabama_business_licenses',
    host: DB13,
    stage_db: 'usa_raw',
    raw_city_state_zip_column: 'registered_office_city_state_zip',
    raw_city_column: 'registered_office_city',
    raw_state_column: 'registered_office_state',
    raw_zip_column: 'registered_office_zip',
    clean_city_column: 'registered_office_city_clean',
    city_org_id_column: 'registered_office_city_org_id',
    raw_address1_column: 'registered_office_address',
    # raw_address2_column: '',
    clean_address_column: 'registered_office_address_clean',
}

SOURCE_TABLE_CL_COLUMNS = [
    {
        name: 'principal_address_clean',
        after: 'principal_address',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'principal_city',
        after: 'principal_city_state_zip',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'principal_state',
        after: 'principal_city',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'principal_zip',
        after: 'principal_state',
        description: 'VARCHAR(5) DEFAULT NULL',
    },
    {
        name: 'principal_city_clean',
        after: 'principal_zip',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'principal_city_org_id',
        after: 'principal_city_clean',
        description: 'VARCHAR(15) DEFAULT NULL',
    },
    {
        name: 'registered_office_address_clean',
        after: 'registered_office_address',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'registered_office_city',
        after: 'registered_office_city_state_zip',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'registered_office_state',
        after: 'registered_office_city',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'registered_office_zip',
        after: 'registered_office_state',
        description: 'VARCHAR(5) DEFAULT NULL',
    },
    {
        name: 'registered_office_city_clean',
        after: 'registered_office_zip',
        description: 'VARCHAR(255) DEFAULT NULL',
    },
    {
        name: 'registered_office_city_org_id',
        after: 'registered_office_city_clean',
        description: 'VARCHAR(15) DEFAULT NULL',
    },
]

def execute(options = {})
  begin
    route_db13 = C::Mysql.on(DB13, 'usa_raw')

    route_db02 = C::Mysql.on(DB02, 'hle_resources')

    where_part = options['where']

    check_clean_column_exist(CL_ENTITY_NAME_TBL_INFO, SOURCE_TABLE_CL_COLUMNS, route_db13)

    case options['mode']
    when 'names'
      where_ent_part = where_part.nil? ? "#{CL_ENTITY_NAME_TBL_INFO[:scrape_date_column]}>='#{get_recent_date(CL_ENTITY_NAME_TBL_INFO, route_db13)}'" : where_part
      clean_org_names(CL_ENTITY_NAME_TBL_INFO, where_ent_part, route_db13)
      where_ag_part = where_part.nil? ? "#{CL_AGENT_NAME_TBL_INFO[:scrape_date_column]}>='#{get_recent_date(CL_AGENT_NAME_TBL_INFO, route_db13)}'" : where_part
      clean_mixed_names(CL_AGENT_NAME_TBL_INFO, where_ag_part, route_db13)
    when 'cities'
      split_city_state_zip(CL_PRINC_CITY_TBL_INFO, where_part, route_db13)
      clean_cities(CL_PRINC_CITY_TBL_INFO, where_part, route_db13, route_db02)
      address_cleaning(CL_PRINC_CITY_TBL_INFO, where_part, route_db13)
      message_to_slack("Principal cities and addresses were cleaned.")
      split_city_state_zip(CL_OFF_CITY_TBL_INFO, where_part, route_db13)
      clean_cities(CL_OFF_CITY_TBL_INFO, where_part, route_db13, route_db02)
      address_cleaning(CL_OFF_CITY_TBL_INFO, where_part, route_db13)
      message_to_slack("Registered office cities and addresses were cleaned.")
    else
      nil
    end
  rescue => e
    print "#{e} ~> #{e.backtrace.join("\n")}\n"
    message_to_slack("Cleaning process ERROR: #{e} ~> #{e.backtrace.join('\n')}")
  ensure
    route_db13.close
    route_db02.close
  end
end

def escape(str)
  str = str.to_s
  return if str.empty?
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #11] Alabama Business Licenses* \n>#{message}",
      as_user: true
  )
end

def insert(db, tab, h, ignore = false, log=false)
  query = <<~SQL
    INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')})
    VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});
  SQL
  puts query.red if log
  db.query(query)
end

def get_recent_date(table_info, route)
  query = <<~SQL
    SELECT MAX(#{table_info[:clean_scrape_date_column]}) AS #{table_info[:clean_scrape_date_column]}
    FROM #{table_info[:clean_table_name]};
  SQL
  # puts query.green
  result = route.query(query).to_a.first[table_info[:clean_scrape_date_column]]
  if result.nil?
    Date.new(2020,1,1)
  else
    result
  end
end

def clean_org_names(table_info, where_part, route)
  query = <<~SQL
    SELECT t.#{table_info[:raw_name_column]}, MIN(DATE(#{table_info[:scrape_date_column]})) AS scrape_date
    FROM #{table_info[:source_table_name]} t
      LEFT JOIN #{table_info[:clean_table_name]} c on t.#{table_info[:raw_name_column]}=c.#{table_info[:raw_name_column]}
    WHERE t.#{table_info[:raw_name_column]} IS NOT NULL
      AND c.#{table_info[:raw_name_column]} IS NULL
      #{"AND #{where_part}" if where_part}
    GROUP BY #{table_info[:raw_name_column]}
    ORDER BY DATE(#{table_info[:scrape_date_column]});
  SQL
  # puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    # puts "There is no any new *#{table_info[:raw_name_column]}* in *#{table_info[:source_table_name]}* table."
    return
  end
  names_to_clean.each do |row|
    clean_name = row
    # puts "#{clean_name[table_info[:raw_name_column]]}".cyan
    result_name = row[table_info[:raw_name_column]].gsub(/(?<=[a-z])(\?|�)(?=s)/i, "'").strip
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1].strip
    end
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
    case result_name.count('"')
    when 1
      result_name = result_name.sub('"', '')
    when 2
      result_name = result_name.sub('", "', ', ')
    else
      nil
    end
    clean_name[table_info[:clean_name_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC')
    # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_name_column]].match?(/[a-z]/i)
    # puts JSON.pretty_generate(clean_name).yellow
    insert(route, table_info[:clean_table_name], clean_name, true, true)
  end
  message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

# def clean_entity_types(where_part, route)
#   query = <<~SQL
#     SELECT DISTINCT entity_type, DATE(last_scrape_date) AS scrape_date
#     FROM #{SOURCE_TABLE1}
#     WHERE #{where_part}
#       AND entity_type IS NOT NULL
#       AND entity_type NOT IN (SELECT entity_type FROM #{CL_TABLE_ENTITY_TYPES});
#   SQL
#   puts query.green
#   types_to_clean = route.query(query).to_a
#   if types_to_clean.empty?
#     puts "There is no any new entity types in the source table."
#     return
#   end
#   types_to_clean.each do |row|
#     clean_name = row
#     clean_name['entity_type_clean'] = clean_name['entity_type'].split.map(&:capitalize).join(' ')
#     puts JSON.pretty_generate(clean_name).yellow
#     insert(route, CL_TABLE_ENTITY_TYPES, clean_name, true, true)
#   end
# end

def clean_mixed_names(table_info, where_part, route)
  query = <<~SQL
    SELECT t.#{table_info[:raw_name_column]}, MIN(DATE(#{table_info[:scrape_date_column]})) AS scrape_date
    FROM #{table_info[:source_table_name]} t
      LEFT JOIN #{table_info[:clean_table_name]} c on t.#{table_info[:raw_name_column]}=c.#{table_info[:raw_name_column]}
    WHERE t.#{table_info[:raw_name_column]} IS NOT NULL
      AND c.#{table_info[:raw_name_column]} IS NULL
      #{"AND #{where_part}" if where_part}
    GROUP BY #{table_info[:raw_name_column]};
  SQL
  # puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    # puts "There is no any new #{table_info[:raw_name_column]} in #{table_info[:source_table_name]} table."
    return
  end
  det = MiniLokiC::Formatize::Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    # puts "#{clean_name[table_info[:raw_name_column]]}".cyan
    result_name = row[table_info[:raw_name_column]].strip
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end
    if table_info[:person_name_with_comma] && result_name.include?(',') == false
      clean_name[table_info[:clean_name_type_column]] = 'Organization'
    else
      clean_name[table_info[:clean_name_type_column]] = det.determine(result_name)
    end

    if clean_name[table_info[:clean_name_type_column]] == 'Person'
      result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, table_info[:person_name_reverse])
    else
      result_name =  MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
    end
    case result_name.count('"')
    when 1
      result_name = result_name.sub('"', '')
    when 2
      result_name = result_name.sub('", "', ', ')
    else
      nil
    end
    clean_name[table_info[:clean_name_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC').sub('Â¿', "'")
    clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_name_column]].match?(/[a-z]/i)
    # puts JSON.pretty_generate(clean_name).yellow
    insert(route, table_info[:clean_table_name], clean_name, true, true)
  end
  message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

#############################################

def clean_cities(table_info, where_part, route, route_db02)
  states_list = get_states_list(table_info, where_part, route)
  # puts states_list
  return nil if states_list.empty?
  states_list.each do |state|
    cities_to_clean = get_cities_to_clean(state[table_info[:raw_state_column]], table_info, where_part, route)
    cities_to_clean.each do |row|
      # puts JSON.pretty_generate(row).green
      clean_name = row
      city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(row[table_info[:raw_city_column]], state[table_info[:raw_state_column]], 1) if row[table_info[:raw_city_column]].length > 5
      city_name ||= MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(clean_name[table_info[:raw_city_column]]))
      # puts city_name.black.on_red
      result = get_city_org_id(state[table_info[:raw_state_column]], city_name, clean_name[table_info[:raw_zip_column]], route_db02)
      if result.nil?
        next
      else
        clean_name[table_info[:clean_city_column]] = result['short_name']
        clean_name[table_info[:city_org_id_column]] = result['pl_production_org_id']
      end
      # puts "#{clean_name[table_info[:raw_city_column]]} -- #{clean_name[table_info[:clean_city_column]]} -- #{clean_name[table_info[:city_org_id_column]]}".yellow
      update_cities(state[table_info[:raw_state_column]], clean_name, table_info, where_part, route)
    end
  end
end

def get_states_list(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_state_column]}
    FROM #{table_info[:table_name]}
    WHERE #{table_info[:clean_city_column]} IS NULL
      AND #{table_info[:raw_state_column]} IS NOT NULL
      AND #{table_info[:raw_state_column]}<>''#{"\n  AND #{where_part}" if where_part};
  SQL
  # puts query.yellow
  route.query(query).to_a
end

def get_cities_to_clean(state, table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_city_column]}, LEFT(#{table_info[:raw_zip_column]}, 5) AS #{table_info[:raw_zip_column]}
    FROM #{table_info[:table_name]}
    WHERE #{table_info[:clean_city_column]} IS NULL
      AND #{table_info[:raw_city_column]} IS NOT NULL
      AND #{table_info[:raw_state_column]}='#{state}'
      #{"AND #{where_part}" if where_part} 
    ORDER BY #{table_info[:raw_city_column]};
  SQL
  # puts query.magenta
  route.query(query).to_a
end

def update_cities(state, city_data, table_info, where_part, route)
  query = <<~SQL
    UPDATE #{table_info[:table_name]}
    SET #{table_info[:clean_city_column]} = '#{escape(city_data[table_info[:clean_city_column]])}',
      #{table_info[:city_org_id_column]} = '#{city_data[table_info[:city_org_id_column]]}'
    WHERE #{table_info[:raw_city_column]} = '#{escape(city_data[table_info[:raw_city_column]])}'
      AND #{table_info[:clean_city_column]} IS NULL
      AND #{table_info[:raw_state_column]}='#{state}'
      AND #{table_info[:raw_zip_column]} LIKE '#{city_data[table_info[:raw_zip_column]]}%'
      #{"AND #{where_part}" if where_part};
  SQL
  # puts query.red
  route.query(query)
end

def get_city_org_id(state_code, city, zip, route)
  query = <<~SQL
    SELECT short_name, county_name AS county, pl_production_org_id
    FROM hle_resources.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                      FROM hle_resources.usa_administrative_division_states
                      WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  return nil if res.empty?
  return res.first if res.count == 1
  query = <<~SQL
    SELECT *
    FROM hle_resources.zipcode_data
    WHERE zip='#{zip}'
      AND primary_city='#{escape(city)}'
      AND state='#{state_code}';
  SQL
  # puts query.green
  res = route.query(query).to_a
  return nil if res.empty?
  zip_data = res.first
  # pl = Pipeline_shapes.new
  # shapes = pl.get_shapes(zip_data['lat'], zip_data['lon'])
  pl = Pipeline::ShapesClient.new
  shapes = JSON.parse(pl.get_shapes({lat: zip_data['lat'], lon: zip_data['lon']}).body)
  county = shapes.filter{|item| item['type'] == 'county'}
  place = shapes.filter{|item| %w(city town village borough township CDP).include?(item['type'])}
  # puts shapes
  if county.count == 1 && place.count == 1
    query = <<~SQL
      SELECT short_name, pl_production_org_id
      FROM hle_resources.usa_administrative_division_counties_places_matching
      WHERE state_name=(SELECT name
                        FROM hle_resources.usa_administrative_division_states
                        WHERE short_name='#{state_code}')
        AND short_name='#{escape(city)}'
        AND bad_matching IS NULL
        AND kind='#{place.first['type']}'
        AND pl_production_org_id IS NOT NULL;
    SQL
    res = route.query(query).to_a
    res.empty? ? nil : res.first
  else
    nil
  end

end

def check_clean_column_exist(table_info, column_list, route)
  column_list.each do |item|
    query = <<~SQL
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA='#{table_info[:stage_db]}'
        AND TABLE_NAME='#{table_info[:source_table_name]}'
        AND COLUMN_NAME='#{item[:name]}';
    SQL
    # puts query.green
    if route.query(query).to_a.empty?
      query = <<~SQL
        ALTER TABLE #{table_info[:source_table_name]}
        ADD COLUMN #{item[:name]} #{item[:description]} AFTER #{item[:after]};
      SQL
      puts "Adding column #{item[:name]} to the source table #{table_info[:source_table_name]}"
      puts query.red
      route.query(query)
    end
  end
end

def address_cleaning(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_address1_column]}#{", #{table_info[:raw_address2_column]}" if table_info[:raw_address2_column]}
    FROM #{table_info[:table_name]}
    WHERE #{table_info[:raw_address1_column]} IS NOT NULL
      AND #{table_info[:clean_address_column]} IS NULL
      #{"AND #{where_part}" if where_part} 
    LIMIT 10000;
  SQL
  # puts query
  address_list = route.query(query).to_a
  while address_list.empty? == false
    @semaphore = Mutex.new
    threads = Array.new(5) do
      Thread.new do
        # thread_route = Route_noprefix.new(host: table_info[:host], stage_db: table_info[:stage_db])
        # thread_route.query("use #{thread_route.stage_db}")
        thread_route = C::Mysql.on(table_info[:host], table_info[:stage_db])
        loop do
          item = nil
          @semaphore.synchronize {
            item = address_list.pop
          }
          break if item.nil? && address_list.empty?
          address_to_clean = item[table_info[:raw_address1_column]]
          unless item[table_info[:raw_address2_column]].nil? || item[table_info[:raw_address2_column]].empty?
            address_to_clean += ", #{item[table_info[:raw_address2_column]]}"
          end
          query_up = <<~SQL
            UPDATE #{table_info[:table_name]}
            SET #{table_info[:clean_address_column]} = '#{escape(MiniLokiC::Formatize::Address.abbreviated_streets(address_to_clean))}'
            WHERE #{table_info[:raw_address1_column]} = '#{escape(item[table_info[:raw_address1_column]])}'
              #{"AND #{table_info[:raw_address2_column]} = '#{escape(item[table_info[:raw_address2_column]])}'" if table_info[:raw_address2_column]}
              AND #{table_info[:clean_address_column]} IS NULL
              #{"AND #{where_part}" if where_part};
          SQL
          # puts query_up.green
          thread_route.query(query_up)
        end
        thread_route.close
      end
    end
    threads.each(&:join)
    address_list = route.query(query).to_a
  end
end

# def simple_cleaning__upcase(table_info, where_part, route)
#   query = <<~SQL
#     SELECT #{table_info[:raw_column]}, MIN(DATE(#{table_info[:scrape_date_column]})) AS scrape_date
#     FROM #{table_info[:source_table_name]}
#     WHERE #{table_info[:raw_column]} IS NOT NULL
#       AND #{table_info[:raw_column]} NOT IN (SELECT #{table_info[:raw_column]} FROM #{table_info[:clean_table_name]})
#       #{"AND #{where_part}" if where_part}
#     GROUP BY #{table_info[:raw_column]};
#   SQL
#   puts query.green
#   list_to_clean = route.query(query).to_a
#   if list_to_clean.empty?
#     puts "There is no any new variations in #{table_info[:source_table_name]}."
#     return
#   end
#   list_to_clean.each do |row|
#     clean_name = row
#     clean_name[table_info[:clean_column]] = clean_name[table_info[:raw_column]].split.map(&:capitalize).join(' ')
#     puts JSON.pretty_generate(clean_name).yellow
#     insert(route, table_info[:clean_table_name], clean_name, true, true)
#   end
#   message_to_slack("Table *#{table_info[:source_table_name]}* was updated.")
# end
#
# def clean_person_names(table_info, where_part, route)
#   query = <<~SQL
#     SELECT #{table_info[:raw_column]}, MIN(DATE(#{table_info[:scrape_date_column]})) AS scrape_date
#     FROM #{table_info[:source_table_name]}
#     WHERE #{table_info[:raw_column]} IS NOT NULL
#       #{"AND #{where_part}" if where_part}
#     GROUP BY #{table_info[:raw_column]};
#   SQL
#   puts query.green
#   names_to_clean = route.query(query).to_a
#   if names_to_clean.empty?
#     puts "There is no any new records in #{table_info[:source_table_name]}."
#     return
#   end
#   names_to_clean.each do |row|
#     clean_name = row
#     puts "#{clean_name[table_info[:raw_column]]}".cyan
#     clean_name[table_info[:clean_column]] = MiniLokiC::Formatize::Cleaner.person_clean(clean_name[table_info[:raw_column]], false)
#     puts JSON.pretty_generate(clean_name).yellow
#     # insert(route, table_info[:clean_table_name], clean_name, true, true)
#   end
# end

def remove_not_alpha(str)
  str.nil? ? str : str.gsub(/\A[\.,\-'"`\s]+|[\.,\-'"`\s]+\z/, '')
end

def split_city_state_zip(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_city_state_zip_column]}
    FROM #{table_info[:table_name]}
    WHERE #{table_info[:raw_city_column]} IS NULL
      AND #{table_info[:raw_city_state_zip_column]} IS NOT NULL
      #{"AND #{where_part}" if where_part}
    ORDER BY #{table_info[:raw_city_state_zip_column]};
  SQL
  # puts query.yellow
  city_state_zip_list = route.query(query).to_a
  return nil if city_state_zip_list.empty?
  city_state_zip_list.each do |item|
    m = item[table_info[:raw_city_state_zip_column]].match(/(.+?),+\s?([a-zA-Z\s]+)?\s*(\d{,5})?/)
    next if m.nil?
    # puts "#{m[0]} ~~~ #{m[1]} ~~~ #{m[2]} ~~~ #{m[3]}"
    query = <<~SQL
      UPDATE #{table_info[:table_name]}
      SET #{table_info[:raw_city_column]} = '#{escape(remove_not_alpha(m[1]))}',
        #{table_info[:raw_state_column]} = '#{remove_not_alpha(m[2])}',
        #{table_info[:raw_zip_column]} = '#{remove_not_alpha(m[3])}'
      WHERE #{table_info[:raw_city_state_zip_column]} = '#{escape(item[table_info[:raw_city_state_zip_column]])}'
        AND #{table_info[:raw_city_column]} IS NULL
        #{"AND #{where_part}" if where_part};
    SQL
    # puts query.red
    route.query(query).to_a
  end
end
