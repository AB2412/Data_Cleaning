# Creator:      Sergii Butrymenko
# Dataset Name: NY Lawyer Status
# Task #:       83
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/474
# Created:      September 2022

# ruby mlc.rb --tool="clean::ny::ny_lawyer_status"
# ruby mlc.rb --tool="clean::ny::ny_lawyer_status" --mode='city'
# ruby mlc.rb --tool="clean::ny::ny_lawyer_status" --mode='zip'
# ruby mlc.rb --tool="clean::ny::ny_lawyer_status" --mode='status'
# ruby mlc.rb --tool="clean::ny::ny_lawyer_status" --mode='law_firm_name'

def execute(options = {})
  route = C::Mysql.on(DB01, 'lawyer_status')
  table_description = {
    city: {
      raw_table: 'ny_newyork_bar',
      clean_table: 'ny_newyork_bar__cities_clean',
      state_column: 'law_firm_state',
      raw_column: 'law_firm_city',
      clean_column: 'law_firm_city_clean',
    },
    # zip: {
    #   raw_table: 'ny_newyork_bar',
    #   raw_column: 'law_firm_zip',
    #   clean_column: 'law_firm_zip5',
    # },
    status: {
      raw_table: 'ny_newyork_bar',
      clean_table: 'ny_newyork_bar_status_clean',
      raw_column: 'registration_status',
      clean_column: 'registration_status_clean',
    },
    law_firm_name: {
      raw_table: 'ny_newyork_bar',
      clean_table: 'ny_newyork_bar__law_firm_name_clean',
      raw_column: 'law_firm_name',
      clean_column: 'law_firm_name_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :city
    recent_date = get_recent_date(table_info, route)
    fill_city_table(table_info, recent_date, where_part, route)
    clean_cities(table_info, route)
  when :law_firm_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_law_firm_names(table_info, route)
  when :zip
    zip5(table_info, route)
  when :status
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    check_status(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
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
    text: "*[CLEANING #83] NY Lawyer Status* \n>#{type} #{message}",
    as_user: true
  )
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
      city_org_id = "city_org_id BIGINT(20) DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
      city_org_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         #{city_org_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
         # CHARACTER SET latin1 COLLATE latin1_swedish_ci;
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
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables", :info
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
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date} 
      #{"AND #{where_part}" if where_part} 
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || (names_list.size == 1 && names_list.first[table_info[:raw_column]].nil?)
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
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

# def prepare_city(name)
#   name.sub(/^Last known -\s/i, '').sub(/^rural\s/i, '').sub(/\sTownship$/i, '')
# end

def clean_cities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}, #{table_info[:state_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  city_list = route.query(query).to_a
  return nil if city_list.empty?

  city_hash = {
    'wassaic' => 'Amenia',
    'getzville' => 'Amherst',
    'snyder' => 'Amherst',
    'west falls' => 'Aurora',
    'spencertown' => 'Austerlitz',
    'ballston lake' => 'Ballston',
    'burnt hills' => 'Ballston',
    'bedford corners' => 'Bedford',
    'delmar' => 'Bethlehem',
    'glenmont' => 'Bethlehem',
    'selkirk' => 'Bethlehem',
    'slingerlands' => 'Bethlehem',
    'hogansburg' => 'Bombay',
    'east setauket' => 'Setauket-East Setauket',
    'setauket' => 'Setauket-East Setauket',
    'south setauket' => 'Brookhaven',
    'upton' => 'Brookhaven',
    'old chatham' => 'Chatham',
    'mediacom park' => 'Blooming Grove',
    'sugar loaf' => 'Chester',
    'rexford' => 'Clifton Park',
    'nineveh' => 'Colesville',
    'latham' => 'Colonie',
    'loudonville' => 'Colonie',
    'newtonville' => 'Colonie',
    'cortlandt manor' => 'Cortlandt',
    'wingdale' => 'Dover',
    'cornwallville' => 'Durham',
    'hopewell jct' => 'Hopewell Junction',
    'derby' => 'Evans',
    'woodbourne' => 'Fallsburg',
    'ardsley on hudson' => 'Irvington',
    'ardsley-on-hudson' => 'Irvington',
    'lake view' => 'Hamburg',
    'irving' => 'Hanover',
    'purchase' => 'Harrison',
    'garnerville' => 'Haverstraw',
    'bellerose village' => 'Bellerose',
    'n merrick' => 'Hempstead',
    'north baldwin' => 'Hempstead',
    'west hempsteadny' => 'West Hempstead',
    'west henrietta' => 'Henrietta',
    'barryville' => 'Highland',
    'eldred' => 'Highland',
    'huntingtn sta' => 'Huntington',
    'bayshore' => 'Islip',
    'edgewood' => 'Islip',
    'la fayette' => 'LaFayette',
    'lagrangeville' => 'La Grange',
    'cross river' => 'Lewisboro',
    'goldens bridge' => 'Lewisboro',
    'south salem' => 'Lewisboro',
    'waccabuc' => 'Lewisboro',
    'niagara university' => 'Lewiston',
    'milton' => 'Marlborough',
    'arkville' => 'Middletown',
    'erieville' => 'Nelson',
    'millwood' => 'New Castle',
    'feura bush' => 'New Scotland',
    'arverne' => 'New York',
    'astoria' => 'New York',
    'bayside' => 'New York',
    'bayside hills' => 'New York',
    'beechhurst' => 'New York',
    'belle harbor' => 'New York',
    'breezy point' => 'New York',
    'briarcliff' => 'Briarcliff Manor',
    'briarwood' => 'New York',
    'brooklyn heights' => 'New York',
    'cambria heights' => 'New York',
    'college point' => 'New York',
    'corona' => 'New York',
    'douglaston' => 'New York',
    'east elmhurst' => 'New York',
    'elmhurst' => 'New York',
    'far rockaway' => 'New York',
    'flushing' => 'New York',
    'forest hills' => 'New York',
    'fresh meadows' => 'New York',
    'glen oaks' => 'New York',
    'glendale' => 'New York',
    'hollis' => 'New York',
    'howard beach' => 'New York',
    'jackson heights' => 'New York',
    'jackson hts' => 'New York',
    'jamaica' => 'New York',
    'kew gardens' => 'New York',
    'laurelton' => 'New York',
    'little neck' => 'New York',
    'long island city' => 'New York',
    'maspeth' => 'New York',
    'middle village' => 'New York',
    'neponsit' => 'New York',
    'new york city' => 'New York',
    'newyork' => 'New York',
    'ny' => 'New York',
    'oakland gardens' => 'New York',
    'ozone park' => 'New York',
    'queens village' => 'New York',
    'rego park' => 'New York',
    'richmond hill' => 'New York',
    'ridgewood' => 'New York',
    'riverdale' => 'New York',
    'rockaway beach' => 'New York',
    'rockaway park' => 'New York',
    'rockaway point' => 'New York',
    'rosedale' => 'New York',
    's richmond hl' => 'New York',
    'saint albans' => 'New York',
    'south ozone park' => 'New York',
    'south richmond hill' => 'New York',
    'springfield gardens' => 'New York',
    'sunnyside' => 'New York',
    'whitestone' => 'New York',
    'woodhaven' => 'New York',
    'woodside' => 'New York',
    'broad channel' => 'New York',
    'nyc' => 'New York',
    'ray brook' => 'North Elba',
    'roslyn hts' => 'North Hempstead',
    'purdys' => 'North Salem',
    'n tonawanda' => 'North Tonawanda',
    'gansevoort' => 'Northumberland',
    'palisades' => 'Orangetown',
    'wellesley island' => 'Orleans',
    'fly creek' => 'Otsego',
    'woodbury' => 'Oyster Bay',
    'sauquoit' => 'Paris',
    'garrison' => 'Philipstown',
    'modena' => 'Plattekill',
    'tallman' => 'Ramapo',
    'cottekill' => 'Rosendale',
    'shushan' => 'Salem',
    'melrose' => 'Schaghticoke',
    'mount tremper' => 'Shandaken',
    'saint james' => 'Smithtown',
    'amawalk' => 'Somers',
    'baldwin place' => 'Somers',
    'remsenburg' => 'Southampton',
    'pine island' => 'Warwick',
    'new hampton' => 'Wawayanda',
    'slate hill' => 'Wawayanda',
    'bearsville' => 'Woodstock',
    'jefferson valley' => 'Jefferson Valley-Yorktown',
    'mohegan lake' => 'Yorktown',
  }

  city_list.each do |item|
    puts JSON.pretty_generate(item).green
    city_data = item.dup

    # city_data[table_info[:clean_column]] = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    # city_name = prepare_city(city_data[table_info[:raw_column]]).split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    city_name = city_data[table_info[:raw_column]].
      sub(/\s+on\s+/i, '-on-').
      sub(/\bHL\b/i, 'Hill').
      sub(/\bHTS\b/i, 'Heights').
      sub(/\bJCT\b/i, 'Junction').
      split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, city_data[table_info[:state_column]], 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city_name = correct_city_name
    end
    puts city_name.black.on_red
    if city_data[table_info[:state_column]] == 'NY'
      fixed_city_name = city_name.dup.downcase.sub(/.+/, city_hash)
      city_name = fixed_city_name unless fixed_city_name.empty?
    end

    city_name, city_org_id = get_city_org_id(city_data[table_info[:state_column]], city_name, route)
    puts "#{city_data[table_info[:raw_column]]} -- #{city_name} -- #{city_org_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(city_name)}',
        city_org_id = #{city_org_id.nil? ? "NULL" : "#{city_org_id}"}
      WHERE #{table_info[:raw_column]} = '#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:state_column]}='#{item[table_info[:state_column]]}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    # puts query.red
    route.query(query)
  end
end

def get_city_org_id(state_code, city, route)
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
    [res.first['short_name'], res.first['pl_production_org_id']]
  end
end

def get_state_list(route)
  query = <<~SQL
    SELECT name AS state
    FROM hle_resources_readonly_sync.usa_administrative_division_states;
  SQL
  route.query(query).to_a.map{|i| i['state']}
end

def zip5(table_info, route)
  begin
    query = <<~SQL
      UPDATE #{table_info[:raw_table]}
      SET #{table_info[:clean_column]} = LPAD(LEFT(#{table_info[:raw_column]}, 5), 5, '0')
      WHERE #{table_info[:raw_column]} IS NOT NULL
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    puts query.red
    route.query(query)
  rescue Mysql2::Error
    message_to_slack("Column _#{table_info[:clean_column]}_ doesn't exist in *#{table_info[:raw_table]}* table. Creating it now...")
    add_column = <<~SQL
      ALTER TABLE #{table_info[:raw_table]}
      ADD COLUMN #{table_info[:clean_column]} VARCHAR(5) DEFAULT NULL
      AFTER #{table_info[:raw_column]};
    SQL
    puts add_column.red
    route.query(add_column)
    puts 'Column added'
    retry
  end
end

def fill_status_table(table_info, where_part, route)
  # LEFT JOIN #{table_info[:clean_table]} cl ON IFNULL(r.#{table_info[:raw_status_column]}, '~~~')=IFNULL(cl.#{table_info[:raw_status_column]}, '~~~')
  #   AND IFNULL(r.#{table_info[:raw_type_column]}, '~~~')=IFNULL(cl.#{table_info[:raw_type_column]}, '~~~')
  query = <<~SQL
    SELECT r.#{table_info[:raw_status_column]}, r.#{table_info[:raw_type_column]}
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_status_column]}=cl.#{table_info[:raw_status_column]}
        AND r.#{table_info[:raw_type_column]}=cl.#{table_info[:raw_type_column]}
    WHERE cl.id IS NULL
      AND deleted=0
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_status_column]}, r.#{table_info[:raw_type_column]};
  SQL
  puts query.green
  status_list = route.query(query).to_a
  if status_list.empty? || status_list.first.nil?
    message_to_slack "No new records for *#{table_info[:raw_status_column]}*, *#{table_info[:raw_type_column]}* column found in source tables"
  else
    parts = status_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_status_column]}, #{table_info[:raw_type_column]})
        VALUES
      SQL
      part.each do |item|
        # next if item[table_info[:raw_column]].nil?

        insert_query << "(#{escape_or_null(item[table_info[:raw_status_column]])}, #{escape_or_null(item[table_info[:raw_type_column]])}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
    message_to_slack "#{status_list.size} statuses were added into *db01.lawyer_status.#{table_info[:clean_table]}* and should be cleaned by editors."
  end
end

def check_status(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  status_list = route.query(query).to_a
  message = if status_list.empty?
              "There is no any new status in *#{table_info[:clean_table]}* table."
            else
              "#{status_list.size} statuses in *db01.lawyer_status.#{table_info[:clean_table]}* aren't cleaned."
            end
  message_to_slack message
end

def clean_law_firm_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  law_firm_name_list = route.query(query).to_a
  if law_firm_name_list.empty?
    puts "There is no any new _#{table_info[:raw_column]}_ in *#{table_info[:clean_table]}* table."
    return
  end
  law_firm_name_list.each do |row|
    clean_name = row
    skip_it = 0
    result_name = row[table_info[:raw_column]].sub(/^[-,.]+/, '')
    if result_name.match?(/[^\x00-\x7F]{2}/)
      result_name = result_name.encode("Windows-1252").force_encoding("UTF-8").gsub(/[“”]/, '"').gsub('’', "'").gsub("\u00A0", ' ')
      message_to_slack("Check this name >> #{clean_name[table_info[:raw_column]]} << with >> #{result_name} << name.encode(\"Windows-1252\").force_encoding(\"UTF-8\")", :warning)
      # puts("Check this name >> #{clean_name[table_info[:raw_column]]} << with name.encode(\"Windows-1252\").force_encoding(\"UTF-8\")".red)
    end
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    if result_name.size <= 2 ||
      result_name.match?(/^n[. \/]?a\.?$/i) ||
      !result_name.match?(/[a-z]/i) ||
      result_name.match?(/^\d.+\b(rd.?|road|ave(nue)?|st(.|reet?)?|lane|drive|blvd|square|rue|plaza|place|broadway|floor|sector)\b/i) ||
      result_name.match?(/\b(none|retired)\b/i) ||
      ((result_name.match?(/^city of /i) || result_name.match?(/\scity$/i)) &&
        !result_name.match?(/\b(Admin(istration)?|Agency|Association|Authority|Bank|Board|Bureau|Corporation|Council|Court|Dep(t|artment)|Division|Diocese|FBI|Fund|Government|Inc|Industry|Legal|Office|Parenthood|Partnership|Public|Services?|Systems?|Treasury|United)\b/i))
      skip_it = 1
    end
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.gsub('|', ' '))
    result_name = add_point(result_name)
    clean_name[table_info[:clean_column]] = result_name.sub(/\bAt\b/, 'at').sub(/\bOf\b/, 'of').sub(/^,\s/, '')
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}',
        skip_it=#{skip_it}
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
end

def add_point(name)
  words_list = name.split
  words_list.each_with_index do |word, index|
    if word.match(/^[A-Z]$/) && words_list[index + 1]&.downcase != 'professional'
      words_list[index] = "#{word}."
    end
  end
  words_list.join(' ')
end

