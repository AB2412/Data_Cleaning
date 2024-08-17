# Refactorer: Alex Kuzmenko; Initial Creator: Dmitriy Buzina

def normalize_business(name)
  name = general_normalize(name)
  name.split(' ').each_with_object([]) do |word, obj|
    word = word.strip.squeeze(' ')
    word = mac_mc(mega_capitalize(word))
    word = num_prefix_normalize(word)
    if DOWNCASE.include?(word.downcase)
      if (name =~ /#{word}/i) != 0
        word.downcase!
      end
    elsif (word.match(ABBR_LOCAL) && word.match(NOT_ABBR).nil?) ||
          (word.match((BUSINESS_SUFFIXES_LOCAL)) && word.match(/association|company/i).nil?) ||
           word.match(ROMAN_NR) ||
           word.match(/^[a-z]\.?$/i) ||
           word.match(STATES)
      word.upcase!
    end

    correct_word = CORRECTIONS_LOCAL.select { |e| word.match(e) }.values[0]
    obj << (correct_word ||= word)
  end.select{ |e| e.to_s.length > 0 }.join(' ')
end

def committees_query
  <<~SQL
    SELECT
      id,
      committee_name AS name
    FROM nc_campaign_committees
    WHERE clean_committee_name IS NULL;
  SQL
end

def committees_cleaning
  begin
    db13 = C::Mysql.on(DB13, 'nc_raw')

    committees = db13.query(committees_query).to_a

    committees.each do |committee|
      puts "#{committee['id']} id is processing"
      name = committee['name']
      name = normalize_business(name)
      name = name.gsub(/(\()([^\)]+)$/, '')

      update_query = <<~SQL
        UPDATE nc_campaign_committees
        SET clean_committee_name = #{name.dump}
        WHERE id = #{committee['id']};
      SQL
      db13.query(update_query)
    end
  rescue => e
    puts "#{e} ~> #{e.backtrace.join("\n")}"
  ensure
    db13.close if db13
  end
  puts 'Done'
end
