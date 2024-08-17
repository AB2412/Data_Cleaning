# Creator:      Sergii Butrymenko
# Dataset Name: Minnesota Sex Offenders
# Task #:       73
# Created:      May 2022

# ruby mlc.rb --tool="clean::mn::minnesota_sex_offenders"
# ruby mlc.rb --tool="clean::mn::minnesota_sex_offenders" --mode='name'
# ruby mlc.rb --tool="clean::mn::minnesota_sex_offenders" --mode='city'
# ruby mlc.rb --tool="clean::mn::minnesota_sex_offenders" --mode='crime'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    name: {
      raw_table: 'minnesota_sex_offenders',
      clean_table: 'minnesota_sex_offenders__names_clean',
      raw_column: 'full_name',
      clean_column: 'full_name_clean',
    },
    city: {
      raw_table: 'minnesota_sex_offenders',
      clean_table: 'minnesota_sex_offenders__cities_clean',
      state_column: 'state',
      raw_column: 'city',
      clean_column: 'city_clean',
    },
    crime: {
      raw_table: 'minnesota_sex_offenders',
      clean_table: 'minnesota_sex_offenders__crime_clean',
      raw_column: 'offense_statute',
      clean_column: 'offense_description',
      # local_connection: true
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :name
    table_info = table_description[mode]
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_person_names(table_info, route)
  when :city
    table_info = table_description[mode]
    recent_date = get_recent_date(table_info, route)
    fill_city_table(table_info, recent_date, where_part, route)
    clean_cities(table_info, route)
  when :crime
    crimes_checking(table_info, where_part, route)
  else
    puts 'EMPTY'.black.on_yellow
    # crime_types_cleaning(table_description[:description], where_part, route)
  end
  route.close
end

def crimes_checking(table_info, where_part, route)
  # recent_date = get_recent_date(table_info, route)
  fill_crime_table(table_info, where_part, route)
  check_crimes(table_info, route)
  # clean_crimes(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message, type = '', to = 'one')
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
    text: "*[CLEANING #73] Minnesota Sex Offenders* \n>#{type} #{message}",
    as_user: true
  )
  if to == :all
    Slack::Web::Client.new.chat_postMessage(
      channel: 'U02A6JBK9P1',
      text: "*[CLEANING #73] Minnesota Sex Offenders* \n>#{type} #{message}",
      as_user: true
    )
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
         CHARACTER SET latin1 COLLATE latin1_swedish_ci;
         # CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
    puts create_table.red
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
    FROM #{table_info[:raw_table]} r
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

def clean_person_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  names_list.each do |row|
    clean_name = row
    clean_name[:clean_column] = MiniLokiC::Formatize::Cleaner.person_clean(clean_name[table_info[:raw_column]])

    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[:clean_column])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query.red
    route.query(update_query)
  end
end

def fill_city_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, r.#{table_info[:state_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
        AND r.#{table_info[:state_column]} = cl.#{table_info[:state_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL AND cl.#{table_info[:state_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL AND r.#{table_info[:raw_column]}<>''
      AND r.#{table_info[:state_column]} IS NOT NULL AND r.#{table_info[:state_column]}<>''
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]}, r.#{table_info[:state_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack ":information_source: No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, #{table_info[:state_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        insert_query << "('#{escape(item[table_info[:raw_column]])}','#{escape(item[table_info[:state_column]])}','#{item['scrape_date']}'),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def prepare_city(name)
  name.sub(/^Last known -\s/i, '').sub(/^rural\s/i, '').sub(/\sTownship$/i, '')
end

def clean_cities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}, #{table_info[:state_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  city_list = route.query(query).to_a
  return nil if city_list.empty?

  city_list.each do |item|
    puts JSON.pretty_generate(item).green
    city_data = item.dup
    # city_data[table_info[:clean_column]] = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    city_name = prepare_city(city_data[table_info[:raw_column]]).split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, city_data[table_info[:state_column]], 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city_name = correct_city_name
    end
    puts city_name.black.on_red

    city_name, usa_adcp_matching_id = get_usa_adcp_matching_id(city_data[table_info[:state_column]], city_name, route)
    puts "#{city_data[table_info[:raw_column]]} -- #{city_data[table_info[:clean_column]]} -- #{usa_adcp_matching_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(city_name)}',
        usa_adcp_matching_id = #{usa_adcp_matching_id.nil? ? "NULL" : "#{usa_adcp_matching_id}"}
      WHERE #{table_info[:raw_column]} = '#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:state_column]}='#{item[table_info[:state_column]]}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    # puts query.red
    route.query(query)
  end
end

def get_usa_adcp_matching_id(state_code, city, route)
  query = <<~SQL
    SELECT id, short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    [city, 'NULL']
  else
    [res.first['short_name'], res.first['id']]
  end
end

def fill_crime_table(table_info, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  offense_list = route.query(query).to_a
  if offense_list.empty? || offense_list.first.nil? || (offense_list.count == 1 && offense_list.first[table_info[:raw_column]].nil?)
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = offense_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]})
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        insert_query << "('#{escape(item[table_info[:raw_column]])}'),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
    message_to_slack("#{offense_list.size} offenses were added into *db01.usa_raw.#{table_info[:clean_table]}* and should be cleaned by editors.", :warning)
  end
end

def check_crimes(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL
      AND #{table_info[:raw_column]} IS NOT NULL;
  SQL
  crimes_list = route.query(query).to_a
  if status_list.empty?
    message_to_slack("There is no any new crimes in *#{table_info[:clean_table]}* table.", :info)
  else
    message_to_slack("#{crimes_list.size} offenses in *db01.usa_raw.#{table_info[:clean_table]}* aren't cleaned.", :warning, :all)
  end
end

def clean_crimes(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  crimes_list = route.query(query).to_a
  if crimes_list.empty?
    message_to_slack "There is no any new crimes in *#{table_info[:clean_table]}* table."
    return
  end
  state_list = get_state_list(route)
  total_crime_types = crimes_list.count
  crimes_list.each do |item|
    prepared_description = item
    clean_description = item[table_info[:raw_column]].dup.downcase
    clean_description.sub!(/\b0f\b/, 'of')
    clean_description.sub!(/\babuseof\b/, 'abuse of')
    clean_description.sub!(/\batt\b/, 'attempt')
    clean_description.sub!(/\ba\s*&\s*b\b/i, 'assault and battery')
    clean_description.sub!(/\b(asul|aslt|asslt|assulty)\b/, 'assault')   # a
    clean_description.sub!(/\b(agg(?:ervated|r|rav|avated|raved|rivated)?\.?)(?=\b|\s)\b/, 'aggravated')
    clean_description.sub!(/\b(at|att|atmp)\b/, 'attempt')
    clean_description.sub!(/\baut\b/, 'authority')
    clean_description.sub!(/\b(ba|bat|batt|batter)\b/, 'battery')
    clean_description.sub!(/\bbehaviorwith\b/, 'behavior with')
    clean_description.sub!(/\b(c|ch|chi|chil|chld)\b/, 'child')
    clean_description.sub!(/\bchildr\b/, 'children')
    clean_description.sub!(/\bcoer\b/, 'coercion')
    clean_description.sub!(/\b(comm|comit)\b/, 'commit')
    clean_description.sub!(/\b(crim)\b/, 'criminal')
    clean_description.sub!(/\bcsc\b/, 'criminal sexual conduct')
    clean_description.sub!(/\b(de|deg|degr)\b/, 'degree')
    clean_description.sub!(/\bdev\b/, 'deviate')
    clean_description.sub!(/\bdissem\b/, 'disseminate')
    clean_description.sub!(/\bdonduct\b/, 'conduct')
    clean_description.sub!(/\bfel\b/, 'felony')
    clean_description.sub!(/\bfrc\b/, 'force')
    clean_description.sub!(/\b(juv|juven|juvni|juvenil)\b/, 'juvenile')
    clean_description.sub!(/\bi\b/, 'intent')
    clean_description.sub!(/\b(ind|inde)\b/, 'indecency')
    clean_description.sub!(/\binj\b/, 'injury')
    clean_description.sub!(/\b(interco|intercour)\b/, 'intercourse')
    clean_description.sub!(/\binvol\b/, 'involuntary')
    clean_description.sub!(/\binvl\b/, 'involving')
    clean_description.sub!(/\b(mol|molest)\b/, 'molestation')
    clean_description.sub!(/\bnatur\b/, 'nature')
    clean_description.sub!(/\b(lasciv|lasciviou|lasciviouis)\b/, 'lascivious')
    clean_description.sub!(/\b(m|mi|min|mino)\b/, 'minor')
    clean_description.sub!(/\bmiscd\b/, 'misconduct')
    clean_description.sub!(/\bo12\b/, 'over 16')
    clean_description.sub!(/\bpic\b/, 'picture')
    clean_description.sub!(/\b(por|pornograph)\b/, 'pornography')
    clean_description.sub!(/\bposses\b/, 'possess')
    clean_description.sub!(/\bpru\b/, 'purposes')
    clean_description.sub!(/\bpur\b/, 'purpose')
    clean_description.sub!(/\b(ra|rap)\b/, 'rape')
    clean_description.sub!(/\b(sod|sodom)\b/, 'sodomy')
    clean_description.sub!(/\bu\b/, 'under')
    clean_description.sub!(/\bu16\b/, 'under 16')
    clean_description.sub!(/\b(vic|vict|victi|vctm)\b/, 'victim')
    clean_description.sub!(/\b(w\/?)\b/, 'with')
    clean_description.sub!(/\byoa\b/, 'years of age')
    clean_description.sub!(/\by\b/, 'year')
    clean_description.sub!(/\b(yr|yrs)\b/, 'years')
    clean_description.sub!(/\b(wpn|weapo)\b/, 'weapon')
    clean_description.sub!(/Sex (?=assault|battery|misconduct|exploitation|act)/, 'Sexual ')
    clean_description.sub!('Sexaul', 'Sexual')
    clean_description.sub!('abise', 'abuse')
    clean_description.sub!('againt', 'against')
    clean_description.gsub!(/\b&\b/, 'and')

    clean_description = clean_description.capitalize.sub(/\bus\b/i, 'US').sub(/\b(i+)\b/) {|c| c.upcase}

    state_list.each do |state|
      clean_description.sub!(state.downcase, state)
    end

    prepared_description[table_info[:clean_column]] = clean_description

    puts JSON.pretty_generate(prepared_description).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(prepared_description[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(prepared_description[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack "#{total_crime_types} new crime(s) added to *db01.sex_offenders.#{table_info[:clean_table]}* table."
end

def get_state_list(route)
  query = <<~SQL
    SELECT name AS state
    FROM hle_resources_readonly_sync.usa_administrative_division_states;
  SQL
  route.query(query).to_a.map{|i| i['state']}
end
