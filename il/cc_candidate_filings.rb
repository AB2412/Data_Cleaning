# Creator:      Sergii Butrymenko
# Dataset Name: voters_2016.hyperlocal_new_cc_candidate_filings -- Data Cleaning
# Task #:       67
# Created:      March 2022

# ruby mlc.rb --tool="clean::il::cc_candidate_filings"
# ruby mlc.rb --tool="clean::il::cc_candidate_filings" --mode='district'
# ruby mlc.rb --tool="clean::il::cc_candidate_filings" --mode='cities'
# ruby mlc.rb --tool="clean::il::cc_candidate_filings" --mode='counties'

CL_CITY_TBL_INFO = {
  table_name: 'florida_professional_licenses',
  host: DB01,
  stage_db: 'usa_raw',
  raw_city_column: 'main_city',
  raw_state_column: 'main_state',
  raw_zip_column: 'main_zip',
  clean_city_column: 'main_city_clean',
  city_org_id_column: 'main_city_org_id',
  raw_address1_column: 'main_address',
  # raw_address2_column: '',
  clean_address_column: 'main_address_clean',
  raw_county_column: 'main_county',
  clean_county_column: 'main_county_clean',
}

SOURCE_TABLE_CL_COLUMNS = [
  {
    name: 'main_city_clean',
    after: 'main_city',
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: 'main_city_org_id',
    after: 'main_city_clean',
    description: 'VARCHAR(15) DEFAULT NULL',
  },
  {
    name: 'main_address_clean',
    after: 'main_address',
    description: 'VARCHAR(255) DEFAULT NULL',
  },
  {
    name: 'main_county_clean',
    after: 'main_county',
    description: 'VARCHAR(255) DEFAULT NULL',
  },
]
def execute(options = {})
  route_db01 = C::Mysql.on(DB01, 'voters_2016')
  table_description = {
    holders: {
      raw_table: 'florida_professional_licenses',
      clean_table: 'florida_professional_licenses__names_clean',
      raw_column: 'company_name',
      clean_column: 'company_name_clean',
      type_column: 'company_name_type',
    },
    district: {
      raw_table: 'hyperlocal_new_cc_candidate_filings',
      raw_column: 'office_name',
      clean_column: 'district_clean',
      type_column: 'company_name_type',
    },
  }

  where_part = options['where']


  mode = options['mode']&.to_sym
  table_info = table_description[mode]

  case mode
  when :district
    clean_districts(CL_CITY_TBL_INFO, where_part, route_db01)
  when :holders
    recent_date = get_recent_date(table_info, route_db01)
    fill_table(table_info, recent_date, where_part, route_db01)
    clean_mixed_names(table_info, route_db01)
  when :cities
    check_clean_column_exist(CL_CITY_TBL_INFO[:table_name], SOURCE_TABLE_CL_COLUMNS, route_db01)
    clean_cities(CL_CITY_TBL_INFO, where_part, route_db01)
    address_cleaning(CL_CITY_TBL_INFO, where_part, route_db01)
  else
    message_to_slack('Set proper mode with --mode flag')
  end
  route_db01.close
end

private

def escape(str)
  str.nil? ? str : str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #10] Florida Professional Licenses* \n>#{type} #{message}",
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

def clean_districts(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT office_name, ballot_group
    FROM hyperlocal_new_cc_candidate_filings
    WHERE ballot_group IN ('F', 'J')
      AND district_clean IS NULL;
  SQL
  # puts query
  district_list = route.query(query, symbolize_keys: true).to_a
  return nil if district_list.empty?

  district_list.each do |item|
    district_num = item[:office_name].to_i
    update_query = <<~SQL
      UPDATE hyperlocal_new_cc_candidate_filings
      SET district_clean=#{district_num}
      WHERE ballot_group='#{item[:ballot_group]}'
        AND office_name='#{escape(item[:office_name])}'
        AND district_clean IS NULL;
    SQL
    # puts update_query.red
    route.query(update_query)
  end
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(2),"
      usa_adcp_matching_id = "usa_adcp_matching_id BIGINT(20) DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
    # puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r FORCE INDEX(created_at_company_name_idx)
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || names_list.first.nil? || (names_list.count == 1 && names_list.first[table_info[:raw_column]].nil?)
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_mixed_names(table_info, route)
  # SELECT #{table_info[:raw_column]}
  # FROM florida_professional_licenses__names_clean
  # WHERE company_name <> CONVERT(company_name USING ASCII);
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    puts "There is no any new #{table_info[:raw_column]} in #{table_info[:clean_table]} table."
    return
  end
  det = MiniLokiC::Formatize::Determiner.new
  begin
    names_to_clean.each do |row|
      clean_name = row
      puts "#{clean_name[table_info[:raw_column]]}".cyan
      result_name = row[table_info[:raw_column]].dup.sub(/HOMEOWNERS[\W]* ASSOCIATION/i, "HOMEOWNERS' ASSOCIATION")
      if result_name.match?(/[^\x00-\x7F]{2}/)
        result_name = result_name.encode("Windows-1252").force_encoding("UTF-8").gsub(/[“”]/, '"').gsub('’', "'")
        message_to_slack("Check this name >> #{clean_name[table_info[:raw_column]]} << with name.encode(\"Windows-1252\").force_encoding(\"UTF-8\")", :warning)
        # puts("Check this name >> #{clean_name[table_info[:raw_column]]} << with name.encode(\"Windows-1252\").force_encoding(\"UTF-8\")".red)
      end
      result_name = result_name[1..-1] if result_name[0] == "'" && result_name.count("'") == 1
      # utf8_chars_up.each{|k, v| result_name.gsub!(k ,v)}
      result_name.strip!
      while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
        result_name = result_name[1..-1].strip
      end
      if table_info[:person_name_with_comma] && result_name.include?(',') == false
        clean_name[table_info[:type_column]] = 'Organization'
      else
        clean_name[table_info[:type_column]] = det.determine(result_name)
      end
      result_name =
        if clean_name[table_info[:type_column]] == 'Person'
          MiniLokiC::Formatize::Cleaner.person_clean(result_name, table_info[:person_name_reverse])
        else
          MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
        end
      case result_name.count('"')
      when 1
        result_name = result_name.sub('"', '')
      when 2
        result_name = result_name.sub('", "', ', ')
      else
        nil
      end
      # utf8_chars.each{|k, v| result_name.gsub!(k ,v)}

      clean_name[table_info[:clean_column]] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC').gsub(/(?<![a-z])'[a-z]/, &:upcase).gsub(' DE ', ' de ').gsub(' LA ', ' la ').squeeze("' ").strip
      clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_column]].match?(/[a-z]/i)

      update_query = <<~SQL
        UPDATE #{table_info[:clean_table]}
        SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}',
          #{table_info[:type_column]}='#{escape(clean_name[table_info[:type_column]])}'
        WHERE #{table_info[:clean_column]} IS NULL
          AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
      SQL
      # puts update_query.red
      route.query(update_query)
    end
    message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
  rescue Encoding::UndefinedConversionError => exception
    puts "Error during processing: #{$!}"
    puts "Backtrace:\n\t#{exception.backtrace.join("\n\t")}"
    message_to_slack("Error during processing: #{$!}\n\n```#{exception.backtrace.join("\n")}```", :error)
  end
end


#############################################

def clean_cities(table_info, where_part, route)
  states_list = get_states_list(table_info, where_part, route)
  # puts states_list
  return nil if states_list.empty?
  states_list.each do |state|
    cities_to_clean = get_cities_to_clean(state[table_info[:raw_state_column]], table_info, where_part, route)
    cities_to_clean.each do |row|
      puts JSON.pretty_generate(row).green
      clean_name = row
      city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(row[table_info[:raw_city_column]], state[table_info[:raw_state_column]], 1) if row[table_info[:raw_city_column]].length > 5
      city_name ||= MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(clean_name[table_info[:raw_city_column]])).sub(/\bSPG\b/i, 'Springs')
      puts city_name.black.on_red
      result = get_city_org_id(state[table_info[:raw_state_column]], city_name, clean_name[table_info[:raw_zip_column]], route)
      if result.nil?
        next
      else
        clean_name[table_info[:clean_city_column]] = result['short_name']
        clean_name[table_info[:city_org_id_column]] = result['pl_production_org_id']
      end
      puts "#{clean_name[table_info[:raw_city_column]]} -- #{clean_name[table_info[:clean_city_column]]} -- #{clean_name[table_info[:city_org_id_column]]}".yellow
      update_cities(state[table_info[:raw_state_column]], clean_name, table_info, where_part, route)
    end
  end
end

def clean_counties(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT src.#{table_info[:raw_county_column]}, cl.short_name AS #{table_info[:clean_county_column]}
    FROM #{table_info[:table_name]} src
      JOIN hle_resources_readonly_sync.usa_administrative_division_counties cl ON IF(src.#{table_info[:raw_county_column]}='Dade', 'Miami-Dade', src.#{table_info[:raw_county_column]})=cl.short_name
    WHERE main_state='FL' AND state_id=10 AND src.#{table_info[:clean_county_column]} IS NULL;
  SQL
  puts query.green
  counties_list = route.query(query).to_a
  return nil if counties_list.empty?
  counties_list.each do |county|
    update_query = <<~SQL
      UPDATE #{table_info[:table_name]}
      SET #{table_info[:clean_county_column]} = '#{escape(county[table_info[:clean_county_column]])}'
      WHERE #{table_info[:raw_county_column]} = '#{escape(county[table_info[:raw_county_column]])}'
        AND main_state='FL'
        AND #{table_info[:clean_county_column]} IS NULL
      #{"AND #{where_part}" if where_part};
    SQL
    # puts update_query.red
    route.query(update_query)
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
  puts query.yellow
  route.query(query).to_a
end

def get_cities_to_clean(state, table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_city_column]}
    FROM #{table_info[:table_name]}
    WHERE #{table_info[:clean_city_column]} IS NULL
      AND #{table_info[:raw_city_column]} IS NOT NULL
      AND #{table_info[:raw_state_column]}='#{state}'
      #{"AND #{where_part}" if where_part} 
    ORDER BY #{table_info[:raw_city_column]};
  SQL
  puts query.magenta
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
      #{"AND #{where_part}" if where_part};
  SQL
  puts query.red
  route.query(query)
end

def get_city_org_id(state_code, city, zip, route)
  state_name = if state_code.length == 2
                 <<~SQL
                   (SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
                 SQL
               else
                 "'#{state_code}'"
               end
  query = <<~SQL
    SELECT short_name, county_name AS county, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=#{state_name}
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  return nil if res.empty? || res.count > 1

  return res.first if res.count == 1

  # query = <<~SQL
  #   SELECT *
  #   FROM zipcode_data
  #   WHERE zip='#{zip}'
  #     AND primary_city='#{escape(city)}'
  #     AND state='#{state_code}';
  # SQL
  # # puts query.green
  # res = route.query(query).to_a
  # return nil if res.empty?
  # zip_data = res.first
  # pl = Pipeline_shapes.new
  # shapes = pl.get_shapes(zip_data['lat'], zip_data['lon'])
  # county = shapes.filter{|item| item['type'] == 'county'}
  # place = shapes.filter{|item| %w(city town village borough township CDP).include?(item['type'])}
  # # puts shapes
  # if county.count == 1 && place.count == 1
  #   query = <<~SQL
  #     SELECT short_name, pl_production_org_id
  #     FROM usa_administrative_division_counties_places_matching
  #     WHERE state_name=(SELECT name
  #                       FROM usa_administrative_division_states
  #                       WHERE short_name='#{state_code}')
  #       AND short_name='#{escape(city)}'
  #       AND bad_matching IS NULL
  #       AND kind='#{place.first['type']}'
  #       AND pl_production_org_id IS NOT NULL;
  #   SQL
  #   res = route.query(query).to_a
  #   res.empty? ? nil : res.first
  # else
  #   nil
  # end
end

def check_clean_column_exist(table_name, column_list, route)
  column_list.each do |item|
    query = <<~SQL
      SELECT COLUMN_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA='usa_raw'
        AND TABLE_NAME='#{table_name}'
        AND COLUMN_NAME='#{item[:name]}';
    SQL
    puts query.green
    if route.query(query).to_a.empty?
      query = <<~SQL
        ALTER TABLE #{table_name}
        ADD COLUMN #{item[:name]} #{item[:description]} AFTER #{item[:after]};
      SQL
      puts "Adding column #{item[:name]} to the source table #{table_name}"
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
  puts query
  address_list = route.query(query).to_a
  while address_list.empty? == false
    @semaphore = Mutex.new
    threads = Array.new(5) do
      Thread.new do
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

